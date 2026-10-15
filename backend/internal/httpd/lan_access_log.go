package httpd

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// The LAN listener answers some requests itself — a missing or wrong password,
// a lockout, a request meant for another host, a loopback-only route — before
// the shared router runs. The router's access log therefore never saw them, so
// a phone stuck on a rotated password or locked out left no trace at all; one
// investigation found two 401s in cloudflared's counters and none in the
// daemon log. These are recorded here instead.
//
// Only rejections are logged: everything that reaches the router is already in
// its access log, and the response writer is not wrapped, so streams and
// upgraded connections pass through untouched.

// Request transports, as recorded in log lines.
const (
	transportLoopback   = "loopback"   // the desktop's own listener
	transportLAN        = "lan"        // a client on the network, directly
	transportCloudflare = "cloudflare" // the managed cloudflared connector
	transportConnector  = "connector"  // another tunnel forwarding over loopback
)

type lanRejection struct {
	status int
	code   string
	// token describes the credential presented with a 401: missing, retired
	// (a password this host has since rotated away from), or invalid.
	token string
}

type lanRejectionKey struct{}

// noteLANRejection records why the LAN listener answered r itself. A no-op for
// requests that did not come through lanRejectionLog.
func noteLANRejection(r *http.Request, status int, code string) {
	if rej, ok := r.Context().Value(lanRejectionKey{}).(*lanRejection); ok {
		rej.status, rej.code = status, code
	}
}

func noteLANAuthRejection(r *http.Request, token string) {
	if rej, ok := r.Context().Value(lanRejectionKey{}).(*lanRejection); ok {
		rej.status, rej.code, rej.token = http.StatusUnauthorized, "BAD_PASSWORD", token
	}
}

func lanRejectionLog(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rej := &lanRejection{}
		start := time.Now()
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), lanRejectionKey{}, rej)))
		if rej.status == 0 {
			return
		}
		via := peerTransport(r)
		attrs := []any{
			"method", r.Method,
			"path", r.URL.Path,
			"status", rej.status,
			"code", rej.code,
			"via", via,
			"remote", r.RemoteAddr,
			"duration", time.Since(start),
		}
		if via == transportCloudflare {
			attrs = append(attrs, "visitor", r.Header.Get("CF-Connecting-IP"))
		}
		if rej.token != "" {
			attrs = append(attrs, "token", rej.token)
		}
		log.Info("lan request rejected", attrs...)
	})
}

// peerTransport classifies a request that arrived on the LAN listener by its
// socket peer: only a process on this machine — the managed cloudflared
// connector or another tunnel — can connect from loopback. Cloudflare's
// headers then only name which connector it was, so spoofing them from the
// network gains nothing.
func peerTransport(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		return transportLAN
	}
	if r.Header.Get("Cf-Ray") != "" || r.Header.Get("CF-Connecting-IP") != "" {
		return transportCloudflare
	}
	return transportConnector
}
