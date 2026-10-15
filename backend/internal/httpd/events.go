package httpd

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/go-chi/chi/v5"

	"github.com/aoagents/agent-orchestrator/backend/internal/cdc"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/apispec"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/envelope"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/requestscope"

	"time"
)

const (
	eventsReplayBatch = 512
	eventsLiveBuffer  = 1024
	eventAfterHeader  = "X-AO-Event-After"

	// connectorReplayLimit caps the backlog replayed to a client that reached
	// the daemon through a connector on this machine — the managed cloudflared
	// tunnel above all. One replay batch.
	//
	// Measured on a busy daemon: a phone returning after an hour asked for a
	// 12 MB replay, which a quick tunnel cannot stream (Cloudflare documents
	// Quick Tunnels as not supporting SSE), so the edge cut the stream after
	// ~30s and the phone reconnected for the same backlog, seven times running,
	// with every request it made meanwhile starved behind it. A phone on that
	// path already polls the board and the open conversation, so the skipped
	// history costs it nothing. The cursor jump is reported in
	// X-AO-Event-After, the same signal used when a cursor is ahead of head.
	connectorReplayLimit = eventsReplayBatch
)

type cdcSubscriber interface {
	Subscribe(func(cdc.Event)) (unsubscribe func())
}

// EventsController owns the client-facing CDC stream. Durable replay comes from
// change_log through Source; Broadcaster remains a live-only pub/sub seam.
type EventsController struct {
	Source cdc.Source
	Live   cdcSubscriber
	Log    *slog.Logger
}

// Register mounts the CDC SSE stream route.
func (c *EventsController) Register(r chi.Router) {
	r.Get("/events", c.stream)
}

// eventsHeartbeatInterval paces the idle comment frame. Short enough that a
// buffering intermediary forwards a real event promptly, long enough to be
// negligible on a metered connection. A var so tests need not wait it out.
var eventsHeartbeatInterval = 10 * time.Second

func (c *EventsController) stream(w http.ResponseWriter, r *http.Request) {
	if c.Source == nil || c.Live == nil {
		apispec.NotImplemented(w, r, "GET", "/api/v1/events")
		return
	}

	after, err := parseEventsAfter(r)
	if err != nil {
		envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "INVALID_AFTER",
			"after must be a non-negative integer or latest", nil)
		return
	}
	latestSeq, err := c.Source.LatestSeq(r.Context())
	if err != nil {
		envelope.WriteAPIError(w, r, http.StatusInternalServerError, "internal", "EVENT_CURSOR_FAILED",
			"Could not inspect the event cursor", nil)
		return
	}
	// A cursor ahead of head means the change_log was truncated or replaced. Fall
	// back to head rather than zero: replaying from zero costs the client the whole
	// backlog, and since every connected client is reset at the same moment, they
	// stampede together. Falling back to head loses at most the events in the gap,
	// which clients recover from their next snapshot fetch.
	startAtHead := r.URL.Query().Get("after") == "latest" && r.Header.Get("Last-Event-ID") == ""
	if startAtHead || after > latestSeq {
		after = latestSeq
	}
	requested := after
	via := eventsTransport(r)
	if isConnectorTransport(via) && latestSeq-after > connectorReplayLimit {
		after = latestSeq - connectorReplayLimit
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		envelope.WriteAPIError(w, r, http.StatusInternalServerError, "internal", "SSE_UNSUPPORTED",
			"Streaming is not supported by this server", nil)
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	var overflowed atomic.Bool
	live := make(chan cdc.Event, eventsLiveBuffer)
	unsubscribe := c.Live.Subscribe(func(e cdc.Event) {
		select {
		case live <- e:
		default:
			// Never block the broadcaster. Closing the stream is safer than
			// silently dropping a live event; the client replays on reconnect.
			overflowed.Store(true)
			cancel()
		}
	})
	defer unsubscribe()

	stats := eventStreamStats{via: via, requested: requested, start: after, head: latestSeq, opened: time.Now()}
	c.logger().Info("event stream opened", stats.openAttrs()...)
	defer func() {
		switch {
		case overflowed.Load():
			stats.end = "live_buffer_overflow"
		case r.Context().Err() != nil:
			stats.end = "client_closed"
		default:
			stats.end = "write_failed"
		}
		c.logger().Info("event stream closed", stats.closeAttrs()...)
	}()

	h := w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	h.Set(eventAfterHeader, strconv.FormatInt(after, 10))
	w.WriteHeader(http.StatusOK)
	if startAtHead {
		// Give EventSource a Last-Event-ID before the first live change so an
		// immediate reconnect replays the gap instead of starting at head again.
		if _, err := fmt.Fprintf(w, "id: %d\nevent: cursor\ndata: {}\n\n", after); err != nil { //nolint:gosec // G705: an int64 formatted with %d cannot inject SSE fields.
			return
		}
	}
	flusher.Flush()

	sentSeq := after
	replayed, err := c.replay(ctx, w, flusher, &sentSeq)
	stats.replayed = replayed
	if err != nil {
		return
	}

	// Keep the pipe moving while nothing is happening.
	//
	// An idle stream that writes nothing is fine on a LAN but not behind a
	// proxy: an intermediary buffers a lone small write and has nothing to push
	// it through, so a single event can sit unseen for minutes while the same
	// stream is instant directly. The bulk replay always arrives because it is
	// large enough to flush on its own.
	//
	// A named heartbeat lets remote clients verify that an intermediary actually
	// delivers stream frames; it does not move the cursor.
	heartbeat := time.NewTicker(eventsHeartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, "event: heartbeat\ndata: {}\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-ctx.Done():
			return
		case e := <-live:
			before := sentSeq
			if err := writeSSEEvent(w, flusher, e, &sentSeq); err != nil {
				return
			}
			if sentSeq != before {
				stats.live++
			}
		}
	}
}

// replay writes the durable backlog after *sentSeq and reports how many events
// it sent.
func (c *EventsController) replay(ctx context.Context, w http.ResponseWriter, flusher http.Flusher, sentSeq *int64) (int, error) {
	sent := 0
	for {
		events, err := c.Source.EventsAfter(ctx, *sentSeq, eventsReplayBatch)
		if err != nil {
			return sent, err
		}
		if len(events) == 0 {
			return sent, nil
		}
		for _, e := range events {
			before := *sentSeq
			if err := writeSSEEvent(w, flusher, e, sentSeq); err != nil {
				return sent, err
			}
			if *sentSeq != before {
				sent++
			}
		}
		if len(events) < eventsReplayBatch {
			return sent, nil
		}
	}
}

func parseEventsAfter(r *http.Request) (int64, error) {
	raw := r.URL.Query().Get("after")
	if raw == "latest" || raw == "" {
		// An EventSource reconnects to the same URL; its cursor must override
		// this initial head sentinel or events during the gap would be lost.
		raw = r.Header.Get("Last-Event-ID")
	}
	if raw == "" {
		return 0, nil
	}
	seq, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || seq < 0 {
		return 0, fmt.Errorf("invalid after: %q", raw)
	}
	return seq, nil
}

func writeSSEEvent(w http.ResponseWriter, flusher http.Flusher, e cdc.Event, sentSeq *int64) error {
	if e.Seq <= *sentSeq {
		return nil
	}
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", e.Seq, sseEventName(e.Type), data); err != nil {
		return err
	}
	*sentSeq = e.Seq
	flusher.Flush()
	return nil
}

func sseEventName(t cdc.EventType) string {
	return strings.NewReplacer("\r", "_", "\n", "_").Replace(string(t))
}

func (c *EventsController) logger() *slog.Logger {
	return loggerOrDefault(c.Log)
}

// eventsTransport classifies how a stream request reached the daemon: the
// desktop's loopback listener, or the LAN listener by its socket peer.
func eventsTransport(r *http.Request) string {
	if !requestscope.IsLAN(r.Context()) {
		return transportLoopback
	}
	return peerTransport(r)
}

func isConnectorTransport(via string) bool {
	return via == transportCloudflare || via == transportConnector
}

// eventStreamStats is what the open and close log lines report about one
// stream: where it came from, how far back it started, and what it carried.
type eventStreamStats struct {
	via       string
	requested int64 // cursor the client asked for, after clamping to head
	start     int64 // cursor the replay actually started from
	head      int64
	opened    time.Time
	replayed  int
	live      int
	end       string
}

func (s eventStreamStats) openAttrs() []any {
	return []any{
		"via", s.via,
		"after", s.requested,
		"start", s.start,
		"head", s.head,
		"backlog", s.head - s.start,
		"skipped", s.start - s.requested,
	}
}

func (s eventStreamStats) closeAttrs() []any {
	return []any{
		"via", s.via,
		"replayed", s.replayed,
		"live", s.live,
		"duration", time.Since(s.opened),
		"reason", s.end,
	}
}
