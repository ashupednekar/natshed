package conn

import (
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
)


func CreateOrGetStream() (error, nats.JetStreamContext) {
    nc, err := nats.Connect("nats://localhost:30042")
    if err != nil {
        log.Fatal(err)
    }
    defer nc.Close()
    js, err := nc.JetStream()
    if err != nil {
        log.Fatal(err)
    }
    streamConfig := &nats.StreamConfig{
        Name:     "tasks",
        Subjects: []string{"tasks.>"},
        Retention: nats.WorkQueuePolicy,
        Storage:  nats.FileStorage,
        AllowDirect: true,
        Replicas: 1,
    }
    
    _, err = js.AddStream(streamConfig)
    if err != nil {
        if err == nats.ErrStreamNameAlreadyInUse {
          fmt.Printf("stream exists")
        }
        return err, js
    }
    return nil, js
}

