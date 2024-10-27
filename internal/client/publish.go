package client

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"github.com/ashupednekar/natshed/internal/common"
	"github.com/spf13/cobra"
)

func ParseArgs(cmd *cobra.Command) (string, int, string, string) {
	taskID, err := cmd.Flags().GetString("task-id")
	if err != nil {
		log.Fatal(err)
	}
	if taskID == "" {
		log.Fatal("Error: task-id is required")
	}

  max_iter_str, err := cmd.Flags().GetString("max-occurrences")
  if err != nil {
    log.Fatal(err)
  }
  var max_iter int
  if max_iter_str == "" {
    max_iter = 1
  } else {
    max_iter, err = strconv.Atoi(max_iter_str)
    if err != nil {
      log.Fatalf("Invalid value for max-occurrences: %v", err)
    }
  }

	duration, err := cmd.Flags().GetString("duration")
	if err != nil {
		log.Fatal(err)
	}

	cron_string, err := cmd.Flags().GetString("cron-string")
	if err != nil {
		log.Fatal(err)
	}
  return taskID, max_iter, duration, cron_string
}

func RunClient(cmd *cobra.Command, args []string) {
  nc, js := common.ConnectNATS()
  defer nc.Close()
  taskID, maxIter, duration, cronString := ParseArgs(cmd)	
  payload := common.TaskPayload{
		TaskID:   taskID,
    Schedule: common.Schedule{},
    Iter: 1,
    MaxIter: maxIter, 
	}
  schedule := common.Schedule{CronString: cronString, Duration: duration}
  err := schedule.UpdatePayload(&payload)
  if err != nil{
    log.Fatalf("Error updating payload: %v\n", err)
  }
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Fatalf("Error marshaling payload: %v\n", err)
	}

	consumerName := fmt.Sprintf("consumer-%s", taskID)
  _, consumer_err := js.ConsumerInfo("tasks", consumerName)
	if consumer_err != nil {
		_, err = js.Publish("tasks.internal", payloadBytes)
		if err != nil {
			log.Fatalf("Error publishing to tasks.internal: %v\n", err)
		}
	}
	subject := fmt.Sprintf("tasks.execute.%s", taskID)
	_, err = js.Publish(subject, payloadBytes)
	if err != nil {
		log.Fatalf("Error publishing to %s: %v\n", subject, err)
	}

	fmt.Println("Task scheduled successfully")
}
