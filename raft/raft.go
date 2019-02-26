package raft

import(
	"time"
)

const(
	defaultTimeout time.Duration = 300 * time.Millisecond
)
type Raft struct {
	HeartbeatTimeout time.Duration
}

func NewRaft() *Raft {
	return &Raft{
		HeartbeatTimeout: defaultTimeout,
	}
}