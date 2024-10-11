package main

import (
	"log"
	"os"
	"task_scheduler/internal/client"
	"task_scheduler/internal/worker"

	"github.com/urfave/cli/v2"
)

const NatsURL = "nats://localhost:30042"

func main() {
    app := NewCmd() 
    err := app.Run(os.Args)
    if err != nil {
        log.Fatal(err)
    }
}


func NewCmd() *cli.App {
  return &cli.App{
        Name:  "task-scheduler",
        Usage: "CLI tool to schedule and execute tasks using NATS",
        Commands: []*cli.Command{
            {
                Name:   "client",
                Usage:  "Schedule a task",
                Action: client.RunClient,
                Flags: []cli.Flag{
                    &cli.StringFlag{
                        Name:     "task_id",
                        Usage:    "Unique task ID",
                        Required: true,
                    },
                    &cli.DurationFlag{
                        Name:     "delay",
                        Usage:    "Delay before execution (e.g., 5m for 5 minutes)",
                        Required: true,
                    },
                    &cli.IntFlag{
                        Name:     "max_occurrences",
                        Usage:    "Maximum retry attempts",
                        Value:    3,
                    },
                },
            },
            {
                Name:   "worker",
                Usage:  "Run the worker that listens for tasks",
                Action: worker.RunWorker,
            },
        },
    }
}


