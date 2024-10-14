package common

import "time"

type TaskPayload struct {
	TaskID   string    `json:"task_id"`
	NextExec time.Time `json:"next_execution"`
	AckWait  string    `json:"ack_wait"`
}
