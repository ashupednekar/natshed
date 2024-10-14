package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/ashupednekar/natshed/internal/common"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

func RunWorker(cmd *cobra.Command, args []string) {
  nc, err := nats.Connect(os.Getenv("NATS_URL"))
	if err != nil {
		fmt.Printf("Error connecting to NATS: %v\n", err)
		return
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		fmt.Printf("Error getting JetStream context: %v\n", err)
		return
	}

	// Start internal consumer
	go consumeInternal(js)

	// Keep the main goroutine running
	select {}
}


func consumeInternal(js nats.JetStreamContext) {
	// Create a pull consumer for tasks.internal
	_, err := js.AddConsumer("tasks", &nats.ConsumerConfig{
		Durable:       "internal-consumer",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "tasks.internal",
	})
	if err != nil {
		fmt.Printf("Error creating consumer: %v\n", err)
		return
	}

	sub, err := js.PullSubscribe("tasks.internal", "internal-consumer")
	if err != nil {
		fmt.Printf("Error subscribing to tasks.internal: %v\n", err)
		return
	}

	fmt.Println("Listening for internal tasks...")
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Minute)
		defer cancel()
		msgs, err := sub.Fetch(1, nats.Context(ctx))
		if err != nil {
			if err == nats.ErrTimeout {
				continue
			}
			fmt.Printf("Error fetching message: %v\n", err)
			continue
		}

		for _, msg := range msgs {
			var payload common.TaskPayload
			err := json.Unmarshal(msg.Data, &payload)
			if err != nil {
				fmt.Printf("Error unmarshaling payload: %v\n", err)
				msg.Nak()
				continue
			}

			// Check if consumer exists
			consumerName := fmt.Sprintf("consumer-%s", payload.TaskID)
			_, err = js.ConsumerInfo("tasks", consumerName)
			if err != nil {
				// Consumer doesn't exist, start a new one
				go startTaskConsumer(js, payload.TaskID, payload.AckWait)
			}

			msg.Ack()
		}
	}
}
