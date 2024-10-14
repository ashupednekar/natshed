package common

import "time"

type TaskPayload struct {
	TaskID   string    `json:"task_id"`
	NextExec time.Time `json:"next_execution"`
	AckWait  string    `json:"ack_wait"`
  Iter     int       `json:"iter"`
  MaxIter  int       `json:"max_iter"` 
}
