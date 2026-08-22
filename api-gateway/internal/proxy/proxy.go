package proxy

import (
	"net/http"
	"net/http/httputil"
	"net/url"
)

// NewOrderServiceProxy forwards everything under /orders to the
// Order Service. The gateway's own job is limited to routing plus
// the WebSocket status stream (see internal/wsstream) — it does not
// duplicate the Order Service's idempotency or rate-limiting logic,
// since that would mean two sources of truth for the same guarantee.
func NewOrderServiceProxy(targetBaseURL string) (http.Handler, error) {
	target, err := url.Parse(targetBaseURL)
	if err != nil {
		return nil, err
	}
	return httputil.NewSingleHostReverseProxy(target), nil
}
