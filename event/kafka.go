package event

import (
	"context"
	"encoding/json"
	"fmt"
	"observer/conf"
	"observer/share"
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
	ConsumeTopic(topic, groupId string) (chan *kafka.Message, error)
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
	if len(k.conf.Kafka.BootstrapTopics) > 0 {
		k.bootStrapTopics()
	}
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
func (k *KafkaEventHandler) bootStrapTopics() error {
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": k.conf.Kafka.Brokers,
	})
	if err != nil {
		return fmt.Errorf("failed to create Kafka admin client: %w", err)
	}
	defer admin.Close()

	topics := make([]kafka.TopicSpecification, 0, len(k.conf.Kafka.BootstrapTopics))
	for _, topic := range k.conf.Kafka.BootstrapTopics {
		topics = append(topics, kafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	results, err := admin.CreateTopics(ctx, topics, kafka.SetAdminOperationTimeout(5*time.Second))
	if err != nil {
		return fmt.Errorf("failed to create topics: %w", err)
	}

	for _, res := range results {
		switch res.Error.Code() {
		case kafka.ErrNoError:
			fmt.Printf("Topic '%s' created successfully\n", res.Topic)
		case kafka.ErrTopicAlreadyExists:
			fmt.Printf("Topic '%s' already exists\n", res.Topic)
		default:
			fmt.Printf("Failed to create topic '%s': %s\n", res.Topic, res.Error.String())
		}
	}
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
	// k.producer.Flush(100)
	if err != nil {
		return fmt.Errorf("failed to produce message: %s", err)
	}
	return nil
}

func (k *KafkaEventHandler) Mutate(payload share.MutatePayload) {
	payload.Timestamp = time.Now().Format(time.RFC3339Nano)

	err := k.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &k.conf.Kafka.MutateTopic,
			Partition: kafka.PartitionAny,
		},
		Value: payload.Value,
		Key:   []byte(payload.Index),
		Headers: []kafka.Header{
			{Key: "op", Value: []byte(payload.Op)},
			{Key: "docId", Value: []byte(payload.DocId)},
			{Key: "ts", Value: []byte(payload.Timestamp)},
		},
		Timestamp: time.Now(),
	}, k.mutateChan)
	// k.producer.Flush(100)
	if err != nil {
		k.Log(ErrorLog, fmt.Sprintf("[%s] failed to produce %s - %s command", payload.Timestamp, payload.Index, payload.Op))
	}
}

func (k *KafkaEventHandler) ConsumeTopic(topic, groupId string) (chan *kafka.Message, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": k.conf.Kafka.Brokers,
		"group.id":          groupId,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer %s", err)
	}
	consumer.SubscribeTopics([]string{topic}, nil)
	messages := make(chan *kafka.Message, k.conf.Kafka.MutateChanSize)
	go func() {
		defer close(messages)
		defer consumer.Close()

		for {
			ev := consumer.Poll(50)
			switch e := ev.(type) {
			case *kafka.Message:
				messages <- e
			case kafka.Error:
				k.Log(ErrorLog, fmt.Sprintf(
					"failed to consume topic: %s , error: %s", topic, e.Error()))
				return
			default:
				continue
			}
		}
	}()

	return messages, nil
}
