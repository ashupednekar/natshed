package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"task_scheduler/data"
	"task_scheduler/internal/conn"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/urfave/cli/v2"
)

func RunWorker(c *cli.Context) error {
	err, js := conn.CreateOrGetStream()
	if err != nil {
		log.Fatal(err)
	}

	// Create or get consumer for internal messages
	err = createOrGetConsumer(js, "internal-consumer", "tasks.internal", 30*time.Second, handleInternalMessage)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Worker is listening for tasks...")
	select {} // Block indefinitely
}

func createOrGetConsumer(js nats.JetStreamContext, name string, subject string, ackWait time.Duration, handler func(*nats.Msg)) error {
	// Check if the consumer already exists
	consumerInfo, err := js.ConsumerInfo("tasks", name)
	if err == nil && consumerInfo != nil {
		log.Printf("Consumer %s already exists\n", name)
	} else {
		// Create a filtered consumer for the specific task
		consumerConfig := &nats.ConsumerConfig{
			Durable:       name,
			AckPolicy:     nats.AckExplicitPolicy,
			AckWait:       ackWait,
			DeliverPolicy: nats.DeliverAllPolicy,
			FilterSubject: subject,
		}
		_, err = js.AddConsumer("tasks", consumerConfig)
		if err != nil {
			return fmt.Errorf("failed to create consumer for subject: %s, error: %v", subject, err)
		}
		log.Printf("Consumer created for subject: %s, with ackwait %v\n", subject, ackWait)
	}

	// Subscribe to the subject
	_, err = js.Subscribe(subject, handler)
	if err != nil {
		return fmt.Errorf("failed to subscribe to subject: %s, error: %v", subject, err)
	}
	log.Printf("Subscribed to subject: %s\n", subject)

	return nil
}

func handleInternalMessage(m *nats.Msg) {
	var task data.TaskPayload
	if err := json.Unmarshal(m.Data, &task); err != nil {
		log.Printf("Error unmarshalling task: %v", err)
		return
	}

	err, js := conn.CreateOrGetStream()
	if err != nil {
		log.Fatal(err)
	}

	err = createOrGetConsumer(
		js,
		fmt.Sprintf("consumer-%s", task.TaskID),
		fmt.Sprintf("tasks.execute.%s", task.TaskID),
		task.AckWait,
		handleTaskExecution,
	)
	if err != nil {
		log.Printf("Error creating consumer for task %s: %v", task.TaskID, err)
	}

	m.Ack()
}

func handleTaskExecution(m *nats.Msg) {
	var taskPayload data.TaskPayload
	if err := json.Unmarshal(m.Data, &taskPayload); err != nil {
		log.Printf("Error unmarshalling task: %v", err)
		return
	}

	now := time.Now()
	if taskPayload.ExecutionTimestamp.After(now) {
		fmt.Printf("Skipping task %s, execution time not reached\n", taskPayload.TaskID)
		return
	}

	fmt.Printf("Executing task: %s\n", taskPayload.TaskID)
	m.Ack()
}
