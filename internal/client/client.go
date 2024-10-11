package client

import (
	"encoding/json"
	"fmt"
	"log"
	"task_scheduler/data"
	"task_scheduler/internal/conn"
	"time"
	"github.com/urfave/cli/v2"
)

func RunClient(c *cli.Context) error {
    taskID := c.String("task_id")
    delay := c.Duration("delay")
    maxOccurrences := c.Int("max_occurrences")

    err, js := conn.CreateOrGetStream()
    if err != nil {
        log.Fatal(err)
    }

    taskPayload := data.TaskPayload{
        TaskID:            taskID,
        ExecutionTimestamp: time.Now().Add(delay),
        AckWait:           5 * time.Minute,
        MaxOccurrences:    maxOccurrences,
    }

    payload, err := json.Marshal(taskPayload)
    if err != nil {
        return err
    }

    // Check if a consumer exists for tasks.execute.<taskid>
    _, err = js.ConsumerInfo("tasks", fmt.Sprintf("consumer-%s", taskID))
    if err != nil {
        if _, err := js.Publish("tasks.internal", payload); err != nil {
            return fmt.Errorf("failed to publish to tasks.internal.%s: %v", taskID, err)
        }
        fmt.Printf("No consumer found for task %s, produced to tasks.internal.%s\n", taskID, taskID)
    } else {
        fmt.Printf("Consumer already exists for task %s\n", taskID)
    }

    // Always publish to tasks.execute.<taskid>
    executeSubject := fmt.Sprintf("tasks.execute.%s", taskID)
    if _, err := js.Publish(executeSubject, payload); err != nil {
        return fmt.Errorf("failed to publish to tasks.execute.%s: %v", taskID, err)
    }
    fmt.Printf("Task %s scheduled to execute after %v\n", taskID, delay)

    return nil
}
