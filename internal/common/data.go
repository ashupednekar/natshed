package common

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/robfig/cron"
)

type Schedule struct{
  CronString string  `json:"cron_string"`
  Duration string    `json:"duration"`
}

func (s *Schedule) UpdatePayload(payload *TaskPayload) error {
  ackWait, err := s.GetAckWait()
  if err != nil{
    return err
  }
  payload.NextExec = time.Now().Add(ackWait)
  if s.Duration == "" {
    if s.CronString != ""{
      payload.Schedule = Schedule{
        CronString: s.CronString,
      }
    }else{
      return fmt.Errorf("Error: cron string is required if duration is not passed")
    }
	}else{
    payload.Schedule = Schedule{
      Duration: s.Duration,
    }
  }
  return nil 
}

func (s *Schedule) GetAckWait() (time.Duration, error) {
  var ackWait time.Duration 
  if s.Duration == "" {
    if s.CronString != ""{
      schedule, err := cron.ParseStandard(s.CronString)
      now := time.Now()
      ackWait = schedule.Next(now).Sub(now)
      if err != nil{
        return 0, err
      }
    }else{
      return 0, fmt.Errorf("Error: cron string is required if duration is not passed")
    }
	}else{
    parsedDuration, err := time.ParseDuration(s.Duration)
    ackWait = parsedDuration
    if err != nil {
      return 0, fmt.Errorf("Error parsing duration: %v\n", err)
    }
  }
  return ackWait, nil 
}

type TaskPayload struct {
	TaskID   string    `json:"task_id"`
	NextExec time.Time `json:"next_execution"`
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
  ackWait, err := p.Schedule.GetAckWait()
  if err != nil{
    return err
  }
  fmt.Printf("new ackWait for %s: %v\n", consumerName, ackWait)
  fmt.Printf("consumer info: %v\n", info) 

  js.UpdateConsumer("tasks", &nats.ConsumerConfig{
		Durable:        info.Config.Name,
		AckPolicy:      nats.AckExplicitPolicy,
		AckWait:        ackWait,
		FilterSubject:  info.Config.FilterSubject,
	})

  return nil
}
