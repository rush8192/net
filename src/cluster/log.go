package cluster

import "time"

type LogEntry struct {
	command Command
	term int64
	timestamp time.Time
}