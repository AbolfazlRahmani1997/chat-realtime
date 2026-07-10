package messagebroker

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
)

type Producer struct {
	producer sarama.SyncProducer
	topic    string
}

func NewProducer() (*Producer, error) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	topic := os.Getenv("KAFKA_MESSAGES_TOPIC")
	if topic == "" {
		topic = "chat.messages"
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Flush.Frequency = 500 * time.Millisecond

	producer, err := sarama.NewSyncProducer([]string{brokers}, config)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}

	return &Producer{
		producer: producer,
		topic:    topic,
	}, nil
}

type MessagePayload struct {
	ID           string     `json:"_id"`
	Content      string     `json:"Content"`
	Type         string     `json:"Type"`
	UniqId       string     `json:"UniqId"`
	RoomID       string     `json:"RoomID"`
	Username     string     `json:"Username"`
	ClientID     string     `json:"ClientID"`
	Deliver      []string   `json:"Deliver"`
	Read         []string   `json:"Read"`
	ReplyTo      *ReplyInfo `json:"ReplyTo,omitempty"`
	ForwardFrom  *ForwardInfo `json:"ForwardFrom,omitempty"`
	CreatedAt    time.Time  `json:"CreatedAt"`
	UpdatedAt    time.Time  `json:"UpdatedAt"`
}

type ReplyInfo struct {
	ID       string `json:"id"`
	Content  string `json:"content"`
	Username string `json:"username"`
}

type ForwardInfo struct {
	FromRoomID string `json:"from_room_id"`
	FromMsgID  string `json:"from_msg_id"`
	Content    string `json:"content"`
	Username   string `json:"username"`
}

func (p *Producer) PublishMessage(msg *MessagePayload) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	kafkaMsg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(msg.RoomID),
		Value: sarama.ByteEncoder(data),
	}

	partition, offset, err := p.producer.SendMessage(kafkaMsg)
	if err != nil {
		return fmt.Errorf("kafka send: %w", err)
	}

	log.Printf("kafka: message %s published to partition %d at offset %d", msg.UniqId, partition, offset)
	return nil
}

func (p *Producer) Close() error {
	return p.producer.Close()
}

func SetupProducer() (*Producer, error) {
	kafkaEnabled := os.Getenv("KAFKA_ENABLED")
	if kafkaEnabled == "" || kafkaEnabled == "false" {
		log.Println("Kafka producer disabled (KAFKA_ENABLED != true), messages will be written directly to DB")
		return nil, nil
	}
	return NewProducer()
}
