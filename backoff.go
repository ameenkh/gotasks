package gotasks

import (
	"math/rand/v2"
	"time"
)

// Backoff decides how long to wait before retry attempt+1, given that
// `attempt` attempts have already happened (attempt >= 1 on first failure).
type Backoff interface {
	Next(attempt int) time.Duration
}

// BackoffFunc adapts a function to the Backoff interface.
type BackoffFunc func(attempt int) time.Duration

func (f BackoffFunc) Next(attempt int) time.Duration { return f(attempt) }

// FixedBackoff waits the same duration after every failure.
func FixedBackoff(d time.Duration) Backoff {
	return BackoffFunc(func(int) time.Duration { return d })
}

// LinearBackoff waits base*attempt (30s, 60s, 90s, ... for base=30s).
func LinearBackoff(base time.Duration) Backoff {
	return BackoffFunc(func(attempt int) time.Duration {
		if attempt < 1 {
			attempt = 1
		}
		return base * time.Duration(attempt)
	})
}

// ExponentialBackoff waits base*2^(attempt-1) capped at max, with up to 25%
// random jitter added to spread thundering herds. This is the default
// (base=30s, max=1h).
func ExponentialBackoff(base, max time.Duration) Backoff {
	return BackoffFunc(func(attempt int) time.Duration {
		if attempt < 1 {
			attempt = 1
		}
		d := base
		for i := 1; i < attempt; i++ {
			d *= 2
			if d >= max {
				d = max
				break
			}
		}
		if d > max {
			d = max
		}
		// up to +25% jitter
		return d + time.Duration(rand.Int64N(int64(d)/4+1))
	})
}
