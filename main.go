package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

const fetchTimeout = 5 * time.Minute
const NatsUrl = "localhost:30042"

type TaskPayload struct {
	TaskID   string    `json:"task_id"`
	NextExec time.Time `json:"next_execution"`
	AckWait  string    `json:"ack_wait"`
}

func main() {
	var rootCmd = &cobra.Command{Use: "task-scheduler"}

	var clientCmd = &cobra.Command{
		Use:   "client",
		Short: "Client for scheduling tasks",
		Run:   runClient,
	}

	var workerCmd = &cobra.Command{
		Use:   "worker",
		Short: "Worker for executing tasks",
		Run:   runWorker,
	}

	clientCmd.Flags().String("task-id", "", "Task ID")
	clientCmd.Flags().String("duration", "5m", "Task duration")

  CreateStream()

	rootCmd.AddCommand(clientCmd, workerCmd)
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func runClient(cmd *cobra.Command, args []string) {
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

	nc, err := nats.Connect(NatsUrl)
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

	payload := TaskPayload{
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

func runWorker(cmd *cobra.Command, args []string) {
  nc, err := nats.Connect(NatsUrl)
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
		Durable:   "internal-consumer",
		AckPolicy: nats.AckExplicitPolicy,
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
    ctx, cancel := context.WithTimeout(context.Background(), fetchTimeout)
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
			var payload TaskPayload
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


func startTaskConsumer(js nats.JetStreamContext, taskID, ackWait string) {
	subject := fmt.Sprintf("tasks.execute.%s", taskID)
	consumerName := fmt.Sprintf("consumer-%s", taskID)
	
	ackDuration, err := time.ParseDuration(ackWait)
	if err != nil {
		fmt.Printf("Error parsing ack wait duration: %v\n", err)
		return
	}

	_, err = js.AddConsumer("tasks", &nats.ConsumerConfig{
		Durable:        consumerName,
		AckPolicy:      nats.AckExplicitPolicy,
		AckWait:        ackDuration,
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
    ctx, cancel := context.WithTimeout(context.Background(), fetchTimeout)
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
			var payload TaskPayload
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
			} else {
				// Skip execution
				msg.Nak()
			}
		}
	}
}

func CreateStream(){
	nc, err := nats.Connect(NatsUrl)
	if err != nil {
		fmt.Printf("Error connecting to NATS: %v\n", err)
		os.Exit(1)
	}
	defer nc.Close()

	// Create JetStream context
	js, err := nc.JetStream()
	if err != nil {
		fmt.Printf("Error getting JetStream context: %v\n", err)
		os.Exit(1)
	}

	// Create or get the 'tasks' stream
	streamConfig := &nats.StreamConfig{
		Name:     "tasks",
		Subjects: []string{"tasks.>"},
	}
	_, err = js.AddStream(streamConfig)
	if err != nil {
		if err != nats.ErrStreamNameAlreadyInUse {
			fmt.Printf("Error creating stream: %v\n", err)
			os.Exit(1)
		}
		// Stream already exists, so we can continue
		fmt.Println("Stream 'tasks' already exists")
	} else {
		fmt.Println("Stream 'tasks' created successfully")
	}
}
