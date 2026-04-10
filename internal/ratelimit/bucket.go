package ratelimit

import (
	"context"
	"sync"
	"time"
)

// Bucket implements a token bucket rate limiter.
// Tokens represent bytes; the bucket refills at a constant rate.
type Bucket struct {
	mu        sync.Mutex
	tokens    float64
	maxTokens float64 // burst size in bytes
	rate      float64 // bytes per second
	lastFill  time.Time
}

// New creates a rate limiter with the given rate (bytes/sec) and burst (bytes).
func New(rateBytesPerSec float64, burstBytes float64) *Bucket {
	return &Bucket{
		tokens:    burstBytes,
		maxTokens: burstBytes,
		rate:      rateBytesPerSec,
		lastFill:  time.Now(),
	}
}

// Wait blocks until n tokens are available or ctx is cancelled.
func (b *Bucket) Wait(ctx context.Context, n int) error {
	b.mu.Lock()
	b.refill()
	need := float64(n)

	if b.tokens >= need {
		b.tokens -= need
		b.mu.Unlock()
		return nil
	}

	// Calculate wait time for missing tokens
	deficit := need - b.tokens
	waitDur := time.Duration(deficit / b.rate * float64(time.Second))
	b.tokens = 0
	b.mu.Unlock()

	select {
	case <-time.After(waitDur):
	case <-ctx.Done():
		return ctx.Err()
	}

	// After waiting, deduct remaining tokens
	b.mu.Lock()
	b.refill()
	b.tokens -= need
	if b.tokens < 0 {
		b.tokens = 0
	}
	b.mu.Unlock()
	return nil
}

// Reset refills the bucket to full burst capacity. Call on seek.
func (b *Bucket) Reset() {
	b.mu.Lock()
	b.tokens = b.maxTokens
	b.lastFill = time.Now()
	b.mu.Unlock()
}

func (b *Bucket) refill() {
	now := time.Now()
	elapsed := now.Sub(b.lastFill).Seconds()
	b.tokens += elapsed * b.rate
	if b.tokens > b.maxTokens {
		b.tokens = b.maxTokens
	}
	b.lastFill = now
}
