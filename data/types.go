package data 

import "time"

type TaskPayload struct {
    TaskID            string    `json:"task_id"`
    ExecutionTimestamp time.Time `json:"execution_timestamp"`
    AckWait           time.Duration `json:"ackwait"`
    MaxOccurrences    int       `json:"max_occurrences"`
}
