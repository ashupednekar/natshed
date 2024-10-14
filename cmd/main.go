package main

import (
	"fmt"
	"os"
	"time"

	"github.com/ashupednekar/natshed/internal/client"
	"github.com/ashupednekar/natshed/internal/common"
	"github.com/ashupednekar/natshed/internal/worker"
	"github.com/spf13/cobra"
)

const fetchTimeout = 5 * time.Minute

func main() {
	var rootCmd = &cobra.Command{Use: "task-scheduler"}

	var clientCmd = &cobra.Command{
		Use:   "client",
		Short: "Client for scheduling tasks",
		Run:   client.RunClient,
	}

	var workerCmd = &cobra.Command{
		Use:   "worker",
		Short: "Worker for executing tasks",
		Run:   worker.RunWorker,
	}

	clientCmd.Flags().String("task-id", "", "Task ID")
	clientCmd.Flags().String("duration", "5m", "Task duration")

  common.CreateStream()

	rootCmd.AddCommand(clientCmd, workerCmd)
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

