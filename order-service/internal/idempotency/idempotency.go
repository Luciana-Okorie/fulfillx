package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/redis/go-redis/v9"
)

// Checker implements the fast-path half of idempotency: a short-lived
// Redis lock that stops two near-simultaneous requests with the same
// Idempotency-Key from both reaching the database. The Postgres
// idempotency_keys table (see internal/db) is the durable source of
// truth; Redis just avoids hitting Postgres on every retry and closes
// the race window between "check" and "insert".
type Checker struct {
	rdb *redis.Client
	ttl time.Duration
}

func NewChecker(rdb *redis.Client) *Checker {
	return &Checker{rdb: rdb, ttl: 10 * time.Minute}
}

func HashBody(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// AcquireLock returns true if this caller won the race to process the
// given idempotency key. SET NX is atomic, so exactly one concurrent
// request gets true; everyone else should poll/read the Postgres
// record instead of proceeding.
func (c *Checker) AcquireLock(ctx context.Context, key string) (bool, error) {
	ok, err := c.rdb.SetNX(ctx, lockKey(key), "1", c.ttl).Result()
	if err != nil {
		return false, err
	}
	return ok, nil
}

func (c *Checker) ReleaseLock(ctx context.Context, key string) error {
	return c.rdb.Del(ctx, lockKey(key)).Err()
}

func lockKey(key string) string {
	return "idem:lock:" + key
}
