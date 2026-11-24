package mailru

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/mailru/mrhash"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/multipart"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func TestListM1Pagination(t *testing.T) {
	const firstPage = `{"kind":"file","home":"/dir/first","hash":"0000000000000000000000000000000000000000"},` +
		`{"kind":"unknown","home":"/dir/skipped"}`
	for _, test := range []struct {
		name       string
		secondPage string
		status     int
		wantError  string
	}{
		{
			name: "Complete",
			secondPage: `{"kind":"file","home":"/dir/last","hash":"0000000000000000000000000000000000000000"},` +
				`{"kind":"folder","home":"/dir/subdir"}`,
		},
		{name: "EmptyPage", wantError: "empty page"},
		{name: "RepeatedPage", secondPage: firstPage, wantError: "repeated entry"},
		{name: "FailedPage", status: http.StatusNotFound, wantError: "offset 2"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodPost, r.Method)
				assert.Equal(t, "/api/m1/folder", r.URL.Path)
				assert.NoError(t, r.ParseForm())
				assert.Equal(t, "/dir", r.PostForm.Get("home"))
				assert.Equal(t, strconv.Itoa(maxInt32), r.URL.Query().Get("limit"))
				var items string
				switch requests.Add(1) {
				case 1:
					assert.Equal(t, "0", r.URL.Query().Get("offset"))
					items = firstPage
				case 2:
					assert.Equal(t, "2", r.URL.Query().Get("offset"))
					if test.status != 0 {
						w.WriteHeader(test.status)
						_, _ = fmt.Fprintf(w, `{"status":%d}`, test.status)
						return
					}
					items = test.secondPage
				default:
					t.Error("unexpected extra page request")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = fmt.Fprintf(w, `{"body":{"kind":"folder","count":{"files":3,"folders":1},"list":[%s]}}`, items)
			}))
			defer server.Close()

			ctx, ci := fs.AddConfig(context.Background())
			f := &Fs{
				ci:     ci,
				srv:    rest.NewClient(server.Client()).SetRoot(server.URL).SetErrorHandler(errorHandler),
				source: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "token"}),
				pacer:  fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(time.Millisecond))),
			}
			entries, err := f.List(ctx, "dir")
			assert.Equal(t, int32(2), requests.Load())
			if test.wantError != "" {
				require.ErrorContains(t, err, test.wantError)
				assert.NotErrorIs(t, err, fs.ErrorDirNotFound)
				assert.Empty(t, entries)
				return
			}
			require.NoError(t, err)
			require.Len(t, entries, 3)
			assert.Equal(t, "dir/first", entries[0].Remote())
			assert.Equal(t, "dir/last", entries[1].Remote())
			assert.Equal(t, "dir/subdir", entries[2].Remote())
		})
	}
}

// TestUploadRetry checks that an upload from a seekable pooled buffer
// whose first attempt fails with a retriable error is retried with the
// full body rather than the drained reader.
func TestUploadRetry(t *testing.T) {
	content := bytes.Repeat([]byte("mailru"), 512)
	wantHash := mrhash.Sum(content)
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		if attempts.Add(1) == 1 {
			// Drop the connection so the client sees a retriable error
			conn, _, err := w.(http.Hijacker).Hijack()
			require.NoError(t, err)
			_ = conn.Close()
			return
		}
		assert.Equal(t, content, body, "retried upload should re-send the full body")
		_, _ = w.Write([]byte(hex.EncodeToString(wantHash) + "\n"))
	}))
	defer server.Close()

	ctx := context.Background()
	f := &Fs{
		srv:         rest.NewClient(server.Client()),
		source:      oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "token"}),
		pacer:       fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(time.Millisecond), pacer.MaxSleep(10*time.Millisecond))),
		shardURL:    server.URL,
		shardExpiry: time.Now().Add(time.Hour),
	}
	o := &Object{fs: f, remote: "remote"}

	rw := multipart.NewRW()
	defer func() { _ = rw.Close() }()
	_, err := rw.Write(content)
	require.NoError(t, err)

	gotHash, err := o.upload(ctx, rw, int64(len(content)))
	require.NoError(t, err)
	assert.Equal(t, wantHash, gotHash)
	assert.Equal(t, int32(2), attempts.Load(), "expected exactly one failed attempt and one retry")
}
