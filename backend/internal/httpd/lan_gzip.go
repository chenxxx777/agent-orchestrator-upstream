package httpd

import (
	"bufio"
	"compress/gzip"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"strings"
	"sync"
)

// lanGzipLevel trades a little ratio for speed. Measured on real payloads:
// conversation pages of 214–433 KB shrink 4.5–5.3x and the 54 KB session list
// 6.4x, at 0.2–1.7 ms of CPU per response.
const lanGzipLevel = 5

var lanGzipWriters = sync.Pool{New: func() any {
	w, _ := gzip.NewWriterLevel(io.Discard, lanGzipLevel)
	return w
}}

// lanGzip compresses JSON responses on the authenticated LAN listener when the
// client accepts gzip.
//
// A phone on the managed tunnel re-reads the session list (~50 KB) and the open
// conversation (200–660 KB) every two seconds, and every byte crosses the
// Mac's upstream to the tunnel edge, the slowest leg of that path. Measured:
// 38 minutes of phone use moved about 450 MB, mostly JSON of exactly that
// kind. Phone HTTP stacks send Accept-Encoding and decode transparently, so
// the app needs no change.
//
// Only JSON is compressed. The event stream must reach the client frame by
// frame, the terminal mux hijacks its connection, and preview files are
// served as they are. The loopback desktop listener is not wrapped at all.
func lanGzip(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead || isUpgradeRequest(r) || !acceptsGzip(r.Header.Get("Accept-Encoding")) {
			next.ServeHTTP(w, r)
			return
		}
		gw := &gzipJSONWriter{ResponseWriter: w}
		defer gw.close()
		next.ServeHTTP(gw, r)
	})
}

func isUpgradeRequest(r *http.Request) bool {
	return r.Header.Get("Upgrade") != "" ||
		strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
}

// acceptsGzip reports whether an Accept-Encoding value allows gzip. An
// explicit q=0 refuses it; a wildcard counts only when gzip is not named.
func acceptsGzip(header string) bool {
	wildcard := false
	for _, part := range strings.Split(header, ",") {
		name, params, _ := strings.Cut(strings.TrimSpace(part), ";")
		q := strings.TrimPrefix(strings.ReplaceAll(strings.TrimSpace(params), " ", ""), "q=")
		zero := q != "" && strings.Trim(q, "0.") == "" // q=0, q=0.0, q=0.000
		switch strings.ToLower(strings.TrimSpace(name)) {
		case "gzip":
			return !zero
		case "*":
			wildcard = !zero
		}
	}
	return wildcard
}

func isJSONContentType(value string) bool {
	mediaType, _, err := mime.ParseMediaType(value)
	if err != nil {
		return false
	}
	return mediaType == "application/json" || strings.HasSuffix(mediaType, "+json")
}

// gzipJSONWriter decides at WriteHeader time, once the handler has set its
// content type, whether this response is compressed.
type gzipJSONWriter struct {
	http.ResponseWriter
	decided bool
	gz      *gzip.Writer
}

func (w *gzipJSONWriter) WriteHeader(code int) {
	if w.decided || code < http.StatusOK {
		// Informational responses precede the real one; decide on that.
		w.ResponseWriter.WriteHeader(code)
		return
	}
	w.decided = true
	h := w.Header()
	if code != http.StatusNoContent && code != http.StatusNotModified &&
		h.Get("Content-Encoding") == "" && isJSONContentType(h.Get("Content-Type")) {
		h.Del("Content-Length")
		h.Set("Content-Encoding", "gzip")
		h.Add("Vary", "Accept-Encoding")
		gz, _ := lanGzipWriters.Get().(*gzip.Writer)
		gz.Reset(w.ResponseWriter)
		w.gz = gz
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *gzipJSONWriter) Write(p []byte) (int, error) {
	if !w.decided {
		w.WriteHeader(http.StatusOK)
	}
	if w.gz != nil {
		return w.gz.Write(p)
	}
	return w.ResponseWriter.Write(p)
}

// Flush pushes compressed bytes through too, so a flushing handler is never
// held back by the compressor.
func (w *gzipJSONWriter) Flush() {
	if w.gz != nil {
		_ = w.gz.Flush()
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack is passed through for handlers that take over the connection without
// an Upgrade header. A response that already started compressing cannot be.
func (w *gzipJSONWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if w.gz != nil {
		return nil, nil, errors.New("hijack after a compressed response started")
	}
	hj, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, http.ErrNotSupported
	}
	return hj.Hijack()
}

// Unwrap lets http.ResponseController reach the connection's own writer.
func (w *gzipJSONWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *gzipJSONWriter) close() {
	if w.gz == nil {
		return
	}
	_ = w.gz.Close()
	w.gz.Reset(io.Discard)
	lanGzipWriters.Put(w.gz)
	w.gz = nil
}
