package mr

import (
	"time"
)

type TaskPhase int

const (
	MapPhase TaskPhase = 0
	ReducePhase TaskPhase = 1
)

type TaskStatus int

const (
	Pending TaskStatus = 0
	Running TaskStatus = 1
	Finished TaskStatus = 2
	Error TaskStatus = 3
)

type Task struct {
	Idx int
	Filename string
	Phase TaskPhase
}

type TaskStat struct {
	status TaskStatus
	startTime time.Time
}

