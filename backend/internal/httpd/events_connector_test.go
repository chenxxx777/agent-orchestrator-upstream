package httpd

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/cdc"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/requestscope"
)

// backlogEventSource is a change log with a head far beyond the cursor a
// returning phone asks for.
type backlogEventSource struct {
	head int64

	mu     sync.Mutex
	afters []int64
}

func (s *backlogEventSource) EventsAfter(_ context.Context, after int64, limit int) ([]cdc.Event, error) {
	s.mu.Lock()
	s.afters = append(s.afters, after)
	s.mu.Unlock()
	var out []cdc.Event
	for seq := after + 1; seq <= s.head && len(out) < limit; seq++ {
		out = append(out, testCDCEvent(seq))
	}
	return out, nil
}

func (s *backlogEventSource) LatestSeq(context.Context) (int64, error) { return s.head, nil }

func (s *backlogEventSource) firstAfter() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.afters) == 0 {
		return -1
	}
	return s.afters[0]
}

// streamThrough serves the events route behind shape, which stands in for how
// a request reaches the daemon: the LAN listener marks its context, and the
// socket's peer address is the trusted fact the transport is derived from.
func streamThrough(t *testing.T, src cdc.Source, shape func(*http.Request) *http.Request, after int64, header http.Header) (*http.Response, *lockedBuffer, context.CancelFunc) {
	t.Helper()
	logs := &lockedBuffer{}
	c := &EventsController{Source: src, Live: &fakeEventSubscriber{}, Log: slog.New(slog.NewTextHandler(logs, nil))}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.stream(w, shape(r))
	}))
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/api/v1/events?after="+strconv.FormatInt(after, 10), nil)
	if err != nil {
		cancel()
		t.Fatalf("new request: %v", err)
	}
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("GET /api/v1/events: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp, logs, cancel
}

func viaLANListener(r *http.Request) *http.Request {
	return r.WithContext(requestscope.WithLAN(r.Context()))
}

func viaLANFrom(remote string) func(*http.Request) *http.Request {
	return func(r *http.Request) *http.Request {
		r = viaLANListener(r)
		r.RemoteAddr = remote
		return r
	}
}

func viaDesktop(r *http.Request) *http.Request { return r }

// A phone returning through the managed tunnel used to ask for its whole
// backlog — 12 MB measured — which the tunnel could not stream. It now gets
// at most one replay batch, and the jump is announced in X-AO-Event-After,
// which the phone already adopts as its cursor.
func TestEventsStreamCapsStaleReplayThroughCloudflared(t *testing.T) {
	src := &backlogEventSource{head: 10_000}
	resp, logs, cancel := streamThrough(t, src, viaLANListener, 100, http.Header{
		"Cf-Ray":           {"8c2f1e0a1b2c3d4e-SIN"},
		"Cf-Connecting-Ip": {"203.0.113.9"},
	})

	wantStart := int64(10_000 - connectorReplayLimit)
	if got := resp.Header.Get(eventAfterHeader); got != strconv.FormatInt(wantStart, 10) {
		t.Fatalf("%s = %q, want %d (head minus one batch)", eventAfterHeader, got, wantStart)
	}
	ids := readSSEIDs(t, resp.Body, connectorReplayLimit)
	if ids[0] != strconv.FormatInt(wantStart+1, 10) {
		t.Fatalf("first id = %q, want %d: history before the cap must not be replayed", ids[0], wantStart+1)
	}
	if last := ids[len(ids)-1]; last != "10000" {
		t.Fatalf("last replayed id = %q, want head 10000", last)
	}
	if got := src.firstAfter(); got != wantStart {
		t.Fatalf("replay started after %d, want %d", got, wantStart)
	}

	cancel()
	waitForLog(t, logs, "event stream closed")
	out := logs.String()
	for _, want := range []string{
		`msg="event stream opened" via=cloudflare after=100 start=9488 head=10000 backlog=512 skipped=9388`,
		`msg="event stream closed" via=cloudflare replayed=512 live=0`,
		`reason=client_closed`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log missing %q:\n%s", want, out)
		}
	}
}

// Any tunnel that forwards to the LAN listener over loopback (frp, ngrok, an
// ssh reverse forward) has the same throughput problem, and only a process on
// this machine can connect to the LAN listener from loopback.
func TestEventsStreamCapsStaleReplayThroughAnyLocalConnector(t *testing.T) {
	src := &backlogEventSource{head: 10_000}
	resp, _, cancel := streamThrough(t, src, viaLANListener, 100, nil)
	defer cancel()

	if got := resp.Header.Get(eventAfterHeader); got != strconv.FormatInt(10_000-connectorReplayLimit, 10) {
		t.Fatalf("%s = %q, want the capped cursor", eventAfterHeader, got)
	}
}

func TestEventsStreamKeepsFullReplayOffConnectors(t *testing.T) {
	for _, tc := range []struct {
		name  string
		shape func(*http.Request) *http.Request
		// Cloudflare's headers mean nothing without a loopback peer: a phone on
		// the network could send them itself.
		header http.Header
	}{
		{name: "desktop loopback listener", shape: viaDesktop},
		{name: "phone on the LAN", shape: viaLANFrom("192.168.1.20:52114")},
		{name: "LAN client spoofing Cloudflare headers", shape: viaLANFrom("192.168.1.20:52114"), header: http.Header{"Cf-Ray": {"spoofed"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := &backlogEventSource{head: 10_000}
			resp, _, cancel := streamThrough(t, src, tc.shape, 100, tc.header)
			defer cancel()

			if got := resp.Header.Get(eventAfterHeader); got != "100" {
				t.Fatalf("%s = %q, want the requested cursor 100", eventAfterHeader, got)
			}
			if ids := readSSEIDs(t, resp.Body, 1); ids[0] != "101" {
				t.Fatalf("first id = %q, want 101 (full replay)", ids[0])
			}
		})
	}
}

func TestEventsStreamReplaysASmallConnectorBacklogInFull(t *testing.T) {
	src := &backlogEventSource{head: 10_000}
	resp, _, cancel := streamThrough(t, src, viaLANListener, 10_000-10, http.Header{"Cf-Ray": {"x"}})
	defer cancel()

	if got := resp.Header.Get(eventAfterHeader); got != "9990" {
		t.Fatalf("%s = %q, want 9990: a backlog within the cap is replayed unchanged", eventAfterHeader, got)
	}
	if ids := readSSEIDs(t, resp.Body, 1); ids[0] != "9991" {
		t.Fatalf("first id = %q, want 9991", ids[0])
	}
}

func waitForLog(t *testing.T, logs *lockedBuffer, substr string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(logs.String(), substr) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("log never contained %q:\n%s", substr, logs.String())
}
