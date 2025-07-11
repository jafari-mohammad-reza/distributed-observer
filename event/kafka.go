package event

import (
	"distributed-observer/conf"
	"distributed-observer/share"
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type LogType string

const (
	InfoLog  LogType = "INFO"
	ErrorLog LogType = "WARNING"
)

type EventHandler interface {
	Connect() error
	Disconnect() error
	Log(level LogType, log string) error
	Mutate(payload share.MutatePayload)
}

func NewEventHandler(conf *conf.Config) EventHandler {
	return NewKafkaEventHandler(conf)
}

type KafkaEventHandler struct {
	conf       *conf.Config
	producer   *kafka.Producer
	logChan    chan kafka.Event
	mutateChan chan kafka.Event
}

func NewKafkaEventHandler(conf *conf.Config) *KafkaEventHandler {
	return &KafkaEventHandler{
		conf:       conf,
		logChan:    make(chan kafka.Event, conf.Kafka.LogChanSize),
		mutateChan: make(chan kafka.Event, conf.Kafka.MutateChanSize),
	}
}

func (k *KafkaEventHandler) Connect() error {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":   k.conf.Kafka.Brokers,
		"client.id":           k.conf.Kafka.ClientId,
		"acks":                "all",
		"delivery.timeout.ms": 5000})

	if err != nil {
		return fmt.Errorf("failed to create producer: %s", err)
	}
	k.producer = producer
	go k.observer()
	return nil
}
func (k *KafkaEventHandler) Disconnect() error {
	if k.producer != nil {
		k.producer.Close()
	}
	k.producer = nil
	return nil
}
func (k *KafkaEventHandler) observer() {
	go k.observeChan(k.logChan)
	go k.observeChan(k.mutateChan)
}
func (k *KafkaEventHandler) observeChan(target chan kafka.Event) {
	for event := range target {
		switch ev := event.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
			} else {
				fmt.Printf("Message delivered to %s [%d] at offset %d\n", *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
			}
		default:
			fmt.Printf("Unexpected event type: %T\n", ev)
		}
	}
}

type LogEvent struct {
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Message   string `json:"message"`
	Source    string `json:"source,omitempty"`
}

func (k *KafkaEventHandler) Log(level LogType, log string) error {
	fmt.Printf("[%s] %s %s\n", time.Now().Format(time.RFC3339Nano), level, log)
	msg := LogEvent{
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Level:     string(level),
		Message:   log,
	}
	data, _ := json.Marshal(msg)
	err := k.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &k.conf.Kafka.LogTopic,
			Partition: kafka.PartitionAny,
		},
		Value: data,
		Key:   []byte(level),
		Headers: []kafka.Header{
			{Key: "level", Value: []byte(level)},
			{Key: "ts", Value: []byte(msg.Timestamp)},
		},
		Timestamp: time.Now(),
	}, k.logChan)
	k.producer.Flush(5000)
	if err != nil {
		return fmt.Errorf("failed to produce message: %s", err)
	}
	return nil
}

func (k *KafkaEventHandler) Mutate(payload share.MutatePayload) {

	payload.Timestamp = time.Now().Format(time.RFC3339Nano)
	topic := fmt.Sprintf("%s_%s", payload.Index, payload.Op) //TODO: when closing an index later in storage we must make sure we delete its topics as well

	err := k.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: payload.Value,
		Key:   []byte(payload.Index),
		Headers: []kafka.Header{
			{Key: "op", Value: []byte(payload.Op)},
			{Key: "ts", Value: []byte(payload.Timestamp)},
		},
		Timestamp: time.Now(),
	}, k.mutateChan)
	k.producer.Flush(5000)
	if err != nil {
		k.Log(ErrorLog, fmt.Sprintf("[%s] failed to produce %s - %s command", payload.Timestamp, payload.Index, payload.Op))
	}
}
