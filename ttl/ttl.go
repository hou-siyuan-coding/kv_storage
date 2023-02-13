package ttl

import (
	"time"
)

type TTL struct {
	startTime time.Time
	lifeCycle time.Duration
	DeadLine  time.Time
}

func NewTTL(lifeCycle time.Duration) *TTL {
	now := time.Now()
	return &TTL{startTime: now, lifeCycle: lifeCycle, DeadLine: now.Add(lifeCycle)}
}

