package httpd

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/mobilebridge"
)

// lockedBuffer collects log output from handler goroutines without a race.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func newLoggedMobileLAN(t *testing.T) (*LANManager, *lockedBuffer, *int) {
	t.Helper()
	logs := &lockedBuffer{}
	calls := 0
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusOK)
	})
	m := NewMobileLAN(inner, "h_B", 0, slog.New(slog.NewTextHandler(logs, nil)), nil)
	m.SetPasswordHash(mobilebridge.HashPassword("retired1"))
	m.SetPasswordHash(mobilebridge.HashPassword("current1"))
	return m, logs, &calls
}

func serveLAN(m *LANManager, req *http.Request) int {
	rec := httptest.NewRecorder()
	m.handler.ServeHTTP(rec, req)
	return rec.Code
}

func TestLANRejectionsAreLogged(t *testing.T) {
	for _, tc := range []struct {
		name   string
		build  func() *http.Request
		status int
		want   []string
	}{
		{
			name:   "no password",
			build:  func() *http.Request { return httptest.NewRequest(http.MethodGet, "/api/v1/sessions", nil) },
			status: http.StatusUnauthorized,
			want:   []string{`msg="lan request rejected"`, "path=/api/v1/sessions", "status=401", "code=BAD_PASSWORD", "via=lan", "token=missing"},
		},
		{
			name: "rotated password",
			build: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, "/api/v1/sessions", nil)
				req.Header.Set("Authorization", "Bearer retired1")
				return req
			},
			status: http.StatusUnauthorized,
			want:   []string{"status=401", "token=retired"},
		},
		{
			name: "wrong password",
			build: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, "/api/v1/sessions", nil)
				req.Header.Set("Authorization", "Bearer nope")
				return req
			},
			status: http.StatusUnauthorized,
			want:   []string{"status=401", "token=invalid"},
		},
		{
			name: "address now held by another host",
			build: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, "/api/v1/sessions", nil)
				req.Header.Set("Authorization", "Bearer current1")
				req.Header.Set("X-AO-Expected-Host-ID", "h_A")
				return req
			},
			status: http.StatusMisdirectedRequest,
			want:   []string{"status=421", "code=HOST_ID_MISMATCH"},
		},
		{
			name:   "loopback-only control route",
			build:  func() *http.Request { return httptest.NewRequest(http.MethodGet, "/api/v1/mobile/status", nil) },
			status: http.StatusNotFound,
			want:   []string{"status=404", "code=ROUTE_NOT_FOUND", "path=/api/v1/mobile/status"},
		},
		{
			name: "through the managed tunnel",
			build: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, "/api/v1/sessions", nil)
				req.RemoteAddr = "127.0.0.1:61234"
				req.Header.Set("Cf-Ray", "8c2f1e0a1b2c3d4e-SIN")
				req.Header.Set("CF-Connecting-IP", "203.0.113.9")
				return req
			},
			status: http.StatusUnauthorized,
			want:   []string{"via=cloudflare", "visitor=203.0.113.9"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, logs, calls := newLoggedMobileLAN(t)
			if got := serveLAN(m, tc.build()); got != tc.status {
				t.Fatalf("status = %d, want %d", got, tc.status)
			}
			if *calls != 0 {
				t.Fatal("a rejected request reached the router")
			}
			out := logs.String()
			for _, want := range tc.want {
				if !strings.Contains(out, want) {
					t.Errorf("log missing %q:\n%s", want, out)
				}
			}
		})
	}
}

func TestLANLockoutIsLogged(t *testing.T) {
	m, logs, _ := newLoggedMobileLAN(t)
	bad := func() *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/sessions", nil)
		req.Header.Set("Authorization", "Bearer nope")
		return req
	}
	for range 5 {
		serveLAN(m, bad())
	}
	if got := serveLAN(m, bad()); got != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429 after five failures", got)
	}
	if out := logs.String(); !strings.Contains(out, "status=429") || !strings.Contains(out, "code=LOCKED_OUT") {
		t.Fatalf("lockout not logged:\n%s", out)
	}
}

// Requests that reach the router are already in its access log; logging them
// here as well would double every phone request.
func TestLANAcceptedRequestsAreNotLoggedTwice(t *testing.T) {
	m, logs, calls := newLoggedMobileLAN(t)
	for _, path := range []string{"/api/v1/sessions", "/api/v1/identity"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer current1")
		if got := serveLAN(m, req); got != http.StatusOK {
			t.Fatalf("%s: status = %d, want 200", path, got)
		}
	}
	if *calls != 2 {
		t.Fatalf("router calls = %d, want 2", *calls)
	}
	if out := logs.String(); strings.Contains(out, "lan request rejected") {
		t.Fatalf("accepted request logged as rejected:\n%s", out)
	}
}
