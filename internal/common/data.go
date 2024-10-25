package common

import (
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

type Schedule struct{
  CronString string  `json:"cron_string"`
  Duration string    `json:"duration"`
}

func (s *Schedule) UpdatePayload(payload *TaskPayload) TaskPayload {
  if s.Duration == "" {
    if s.CronString != ""{
      timeTillCron := time.Second * 5 // TODO: use a cron library to get this value
      payload.NextExec = time.Now().Add(timeTillCron)
      payload.Schedule = Schedule{
        CronString: s.CronString,
      }
    }else{
		  log.Fatal("Error: duration is required")
    }
	}else{
    parsedDuration, err := time.ParseDuration(s.Duration)
    if err != nil {
      log.Fatalf("Error parsing duration: %v\n", err)
    }
    payload.NextExec = time.Now().Add(parsedDuration)
    payload.Schedule = Schedule{
      Duration: s.Duration,
    }
  }
  return *payload 
}

type TaskPayload struct {
	TaskID   string    `json:"task_id"`
	NextExec time.Time `json:"next_execution"`
	AckWait  string    `json:"ack_wait"`
  Schedule Schedule  `json:"schedule"`
  Iter     int       `json:"iter"`
  MaxIter  int       `json:"max_iter"` 

}


func (p *TaskPayload) UpdateAckWait(js nats.JetStreamContext, consumerName string) error {
  info, err := js.ConsumerInfo("tasks", consumerName)
  if err != nil{
    return err
  }

  fmt.Printf("schedule: %v\n", p.Schedule)
  var ackWait time.Duration 
  if p.Schedule.Duration != "" {
    ackWait, err = time.ParseDuration(p.Schedule.Duration)
    if err != nil {
      return err
    }
  } else if p.Schedule.CronString != "" {
    ackWait = time.Second * 5  // TODO: use a cron library to get this value
  } else {
    return fmt.Errorf("invalid schedule config")
  }
  ackWait = time.Second * 30
  fmt.Printf("new ackWait for %s: %v\n", consumerName, ackWait)
  fmt.Printf("consumer info: %v\n", info) 
  _, err = js.PullSubscribe(info.Config.FilterSubject, info.Config.Name, nats.AckWait(ackWait))
  if err != nil{
    return err
  }
  return nil
}
