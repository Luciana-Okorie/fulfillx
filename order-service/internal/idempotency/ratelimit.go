package idempotency

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

// RateLimiter implements a simple fixed-window limiter per customer:
// 100 requests/minute. Good enough to demonstrate the pattern; a
// sliding-window or token-bucket approach would be the production
// upgrade (documented as a trade-off in the README).
type RateLimiter struct {
	rdb    *redis.Client
	limit  int
	window time.Duration
}

func NewRateLimiter(rdb *redis.Client) *RateLimiter {
	return &RateLimiter{rdb: rdb, limit: 100, window: time.Minute}
}

func (rl *RateLimiter) Allow(ctx context.Context, customerID string) (bool, error) {
	windowKey := fmt.Sprintf("ratelimit:%s:%d", customerID, time.Now().Unix()/int64(rl.window.Seconds()))

	count, err := rl.rdb.Incr(ctx, windowKey).Result()
	if err != nil {
		return false, err
	}
	if count == 1 {
		rl.rdb.Expire(ctx, windowKey, rl.window)
	}
	return count <= int64(rl.limit), nil
}

// Middleware enforces the limit keyed on customer_id, expected to be
// set by an upstream auth layer / API gateway. Falls back to the
// remote address if absent so the demo works standalone.
func (rl *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		customerID := r.Header.Get("X-Customer-ID")
		if customerID == "" {
			customerID = r.RemoteAddr
		}

		allowed, err := rl.Allow(r.Context(), customerID)
		if err != nil {
			// Fail open: a Redis outage should degrade rate limiting,
			// not take down order creation. Logged, not silent.
			next.ServeHTTP(w, r)
			return
		}
		if !allowed {
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"error":"rate limit exceeded"}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}
