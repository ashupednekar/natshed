package client

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ashupednekar/natshed/internal/common"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)


func RunClient(cmd *cobra.Command, args []string) {
	taskID, err := cmd.Flags().GetString("task-id")
	if err != nil {
		log.Fatal(err)
	}
	if taskID == "" {
		log.Fatal("Error: task-id is required")
	}

	duration, err := cmd.Flags().GetString("duration")
	if err != nil {
		log.Fatal(err)
	}
	if duration == "" {
		log.Fatal("Error: duration is required")
	}

	nc, err := nats.Connect(os.Getenv("NATS_URL"))
	if err != nil {
		log.Fatalf("Error connecting to NATS: %v\n", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("Error getting JetStream context: %v\n", err)
	}

	// Check if consumer exists
	consumerName := fmt.Sprintf("consumer-%s", taskID)
  _, consumer_err := js.ConsumerInfo("tasks", consumerName)

	parsedDuration, err := time.ParseDuration(duration)
	if err != nil {
		log.Fatalf("Error parsing duration: %v\n", err)
	}

	payload := common.TaskPayload{
		TaskID:   taskID,
		NextExec: time.Now().Add(parsedDuration),
		AckWait:  duration,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Fatalf("Error marshaling payload: %v\n", err)
	}

	if consumer_err != nil {
		// Consumer doesn't exist, publish to tasks.internal
		_, err = js.Publish("tasks.internal", payloadBytes)
		if err != nil {
			log.Fatalf("Error publishing to tasks.internal: %v\n", err)
		}
		fmt.Println("Task scheduled for first time")
	}

	// Publish to tasks.execute.<task_id>
	subject := fmt.Sprintf("tasks.execute.%s", taskID)
	_, err = js.Publish(subject, payloadBytes)
	if err != nil {
		log.Fatalf("Error publishing to %s: %v\n", subject, err)
	}

	fmt.Println("Task scheduled successfully")
}
