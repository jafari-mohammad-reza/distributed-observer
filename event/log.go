package event

import (
	"encoding/json"
	"fmt"
	"observer/conf"
)

type LogServer interface {
	Start() error
}
type KafkaLogServer struct {
	conf         *conf.Config
	eventHandler EventHandler
}

func NewLogServer(conf *conf.Config, eventHandler EventHandler) LogServer {
	return &KafkaLogServer{
		conf:         conf,
		eventHandler: eventHandler,
	}
}

func (k *KafkaLogServer) Start() error {
	go func() {
		fmt.Println("listen for logs")
		messages, err := k.eventHandler.ConsumeTopic(k.conf.Kafka.LogTopic, "log-handler")
		if err != nil {
			fmt.Printf("failed to consume log %s", err.Error())
			k.eventHandler.Log(ErrorLog, err.Error())
		}
		for msg := range messages {
			var log LogEvent
			err := json.Unmarshal(msg.Value, &log)
			if err != nil {
				k.eventHandler.Log(ErrorLog, err.Error())
				return
			}
			// TODO: send logs to storage with index of that day log
		}
	}()
	return nil
}
