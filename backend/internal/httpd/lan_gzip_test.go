package httpd

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/mobilebridge"
)

// conversationLikeJSON is shaped like a conversation page: many messages that
// repeat the same keys, which is why such pages gzip several times over.
func conversationLikeJSON() string {
	var b strings.Builder
	b.WriteString(`{"conversationId":"c1","messages":[`)
	for i := range 400 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"id":"m%d","role":"assistant","sequence":%d,"text":"Reading backend/internal/httpd/events.go to see how the replay is bounded (%d)."}`, i, i, i)
	}
	b.WriteString(`]}`)
	return b.String()
}

func jsonHandler(body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		_, _ = io.WriteString(w, body)
	})
}

// rawGet issues a request without Go's transparent decompression, so the test
// sees exactly what crossed the wire.
func rawGet(t *testing.T, url string, header http.Header) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, body
}

func gunzip(t *testing.T, b []byte) string {
	t.Helper()
	zr, err := gzip.NewReader(strings.NewReader(string(b)))
	if err != nil {
		t.Fatalf("not gzip: %v", err)
	}
	out, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("gunzip: %v", err)
	}
	return string(out)
}

func TestLANGzipCompressesJSONForClientsThatAcceptIt(t *testing.T) {
	body := conversationLikeJSON()
	ts := httptest.NewServer(lanGzip(jsonHandler(body)))
	defer ts.Close()

	resp, wire := rawGet(t, ts.URL, http.Header{"Accept-Encoding": {"gzip, deflate, br"}})
	if resp.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", resp.Header.Get("Content-Encoding"))
	}
	if got := resp.Header.Get("Content-Length"); got == fmt.Sprint(len(body)) {
		t.Fatalf("kept the uncompressed Content-Length %s", got)
	}
	if !strings.Contains(resp.Header.Get("Vary"), "Accept-Encoding") {
		t.Errorf("Vary = %q, want Accept-Encoding", resp.Header.Get("Vary"))
	}
	if got := gunzip(t, wire); got != body {
		t.Fatal("decompressed body differs from the original")
	}
	if len(wire)*3 > len(body) {
		t.Errorf("compressed %d -> %d bytes; expected well over 3x for repetitive JSON", len(body), len(wire))
	}
}

func TestLANGzipLeavesOtherResponsesAlone(t *testing.T) {
	body := conversationLikeJSON()
	for _, tc := range []struct {
		name    string
		handler http.Handler
		header  http.Header
		method  string
	}{
		{name: "client does not accept gzip", handler: jsonHandler(body)},
		{name: "client refuses gzip", handler: jsonHandler(body), header: http.Header{"Accept-Encoding": {"gzip;q=0, identity"}}},
		{name: "HEAD", handler: jsonHandler(body), header: http.Header{"Accept-Encoding": {"gzip"}}, method: http.MethodHead},
		{name: "non-JSON content", header: http.Header{"Accept-Encoding": {"gzip"}}, handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "image/png")
			_, _ = io.WriteString(w, body)
		})},
		{name: "already encoded", header: http.Header{"Accept-Encoding": {"gzip"}}, handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Encoding", "br")
			_, _ = io.WriteString(w, body)
		})},
		{name: "no content", header: http.Header{"Accept-Encoding": {"gzip"}}, handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNoContent)
		})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(lanGzip(tc.handler))
			defer ts.Close()
			method := tc.method
			if method == "" {
				method = http.MethodGet
			}
			req, _ := http.NewRequest(method, ts.URL, nil)
			for k, vs := range tc.header {
				req.Header[k] = vs
			}
			resp, err := (&http.Client{Transport: &http.Transport{DisableCompression: true}}).Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.Header.Get("Content-Encoding") == "gzip" {
				t.Fatal("compressed a response that must pass through as is")
			}
		})
	}
}

// The event stream must reach the client frame by frame. If it were buffered
// in a compressor, a phone would see nothing until the buffer filled.
func TestLANGzipNeverHoldsBackTheEventStream(t *testing.T) {
	release := make(chan struct{})
	ts := httptest.NewServer(lanGzip(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "id: 7\nevent: session_updated\ndata: {}\n\n")
		w.(http.Flusher).Flush()
		<-release
	})))
	defer ts.Close()
	defer close(release)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL, nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := (&http.Client{Transport: &http.Transport{DisableCompression: true}}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.Header.Get("Content-Encoding") != "" {
		t.Fatalf("event stream was encoded as %q", resp.Header.Get("Content-Encoding"))
	}
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil || line != "id: 7\n" {
		t.Fatalf("first line = %q, err = %v; the frame must arrive while the stream is still open", line, err)
	}
}

func TestLANGzipPassesUpgradeRequestsThrough(t *testing.T) {
	var hijacked bool
	ts := httptest.NewServer(lanGzip(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "not a hijacker", http.StatusInternalServerError)
			return
		}
		conn, buf, err := hj.Hijack()
		if err != nil {
			return
		}
		hijacked = true
		_, _ = buf.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
		_ = buf.Flush()
		_ = conn.Close()
	})))
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL, nil)
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSwitchingProtocols || !hijacked {
		t.Fatalf("status = %d hijacked = %v, want the handler to take over the connection", resp.StatusCode, hijacked)
	}
}

func TestAcceptsGzip(t *testing.T) {
	for header, want := range map[string]bool{
		"":                     false,
		"gzip":                 true,
		"gzip, deflate, br":    true,
		"br;q=1.0, gzip;q=0.8": true,
		"GZIP":                 true,
		"gzip;q=0":             false,
		"gzip; q=0.000":        false,
		"identity":             false,
		"*":                    true,
		"*;q=0":                false,
		"*, gzip;q=0":          false,
		"deflate, *;q=0.1":     true,
	} {
		if got := acceptsGzip(header); got != want {
			t.Errorf("acceptsGzip(%q) = %v, want %v", header, got, want)
		}
	}
}

// End to end through the real LAN listener: an authenticated phone gets
// compressed JSON, and the loopback desktop handler underneath is unchanged.
func TestLANManagerServesCompressedJSON(t *testing.T) {
	body := conversationLikeJSON()
	st := &authState{}
	st.setHash(mobilebridge.HashPassword("secret12"))
	m := NewLANManager(jsonHandler(body), st, 0, slog.Default(), nil)
	port, err := m.Start(0)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer m.Stop(context.Background())

	resp, wire := rawGet(t, fmt.Sprintf("http://127.0.0.1:%d/api/v1/sessions/s1/conversation", port), http.Header{
		"Authorization":   {"Bearer secret12"},
		"Accept-Encoding": {"gzip"},
	})
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("status = %d Content-Encoding = %q, want a compressed 200", resp.StatusCode, resp.Header.Get("Content-Encoding"))
	}
	if gunzip(t, wire) != body {
		t.Fatal("decompressed body differs")
	}
}
