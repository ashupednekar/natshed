package common

import (
	"fmt"
	"log"
	"os"

	"github.com/nats-io/nats.go"
)

func ConnectNATS() (*nats.Conn, nats.JetStreamContext) {
	nc, err := nats.Connect(os.Getenv("NATS_URL"))
	if err != nil {
		log.Fatalf("Error connecting to NATS: %v\n", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("Error getting JetStream context: %v\n", err)
	}
	return nc, js
}

func CreateStream(){
	nc, err := nats.Connect(os.Getenv("NATS_URL"))
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
