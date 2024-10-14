package common

import (
	"fmt"
	"os"

	"github.com/nats-io/nats.go"
)



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
