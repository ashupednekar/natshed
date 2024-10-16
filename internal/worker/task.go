package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/ashupednekar/natshed/internal/common"
	"github.com/nats-io/nats.go"
)


func startTaskConsumer(js nats.JetStreamContext, taskID string, ackWait time.Duration) {
	subject := fmt.Sprintf("tasks.execute.%s", taskID)
	consumerName := fmt.Sprintf("consumer-%s", taskID)
	
  _, err := js.AddConsumer("tasks", &nats.ConsumerConfig{
		Durable:        consumerName,
		AckPolicy:      nats.AckExplicitPolicy,
		AckWait:        ackWait,
		FilterSubject:  subject,
	})
	if err != nil {
		fmt.Printf("Error creating consumer: %v\n", err)
		return
	}

	sub, err := js.PullSubscribe(subject, consumerName)
	if err != nil {
		fmt.Printf("Error subscribing to %s: %v\n", subject, err)
		return
	}

	fmt.Printf("Started consumer for task: %s\n", taskID)
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

			now := time.Now()
			if now.After(payload.NextExec) || now.Equal(payload.NextExec) {
				fmt.Printf("EXECUTING TASK: %s\n", payload.TaskID)
				msg.Ack()
        if payload.Iter < payload.MaxIter{
          payload.Iter += 1
          duration, err := time.ParseDuration(payload.AckWait)
          if err != nil{
            log.Fatalf("error parsing duration: %v", err)
          }
          payload.NextExec = time.Now().Add(duration)
          payloadBytes, err := json.Marshal(payload)
          if err != nil{
            log.Fatalf("Error marshaling payload: %v", err)
          }
          subject := fmt.Sprintf("tasks.execute.%s", taskID)
          _, err = js.Publish(subject, payloadBytes)
          if err != nil {
            log.Fatalf("Error publishing to %s: %v\n", subject, err)
          }
        }
			} else {
				// Skip execution
				msg.Nak()
			}
		}
	}
}

