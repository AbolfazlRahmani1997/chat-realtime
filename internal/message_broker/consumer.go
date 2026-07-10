package messagebroker

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
)

type InsertMessageFunc func(id string, content string, msgType string, uniqId string, roomID string, username string, clientID string, deliver []string, read []string, replyToJSON *string, forwardFromJSON *string, createdAt time.Time, updatedAt time.Time)

type Consumer struct {
	consumer     sarama.ConsumerGroup
	topic        string
	groupID      string
	insertMessage InsertMessageFunc
}

func NewConsumer(insertFn InsertMessageFunc) (*Consumer, error) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	topic := os.Getenv("KAFKA_MESSAGES_TOPIC")
	if topic == "" {
		topic = "chat.messages"
	}

	groupID := os.Getenv("KAFKA_CONSUMER_GROUP")
	if groupID == "" {
		groupID = "chat-writers"
	}

	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Return.Errors = true
	config.Consumer.MaxProcessingTime = 500 * time.Millisecond
	config.Consumer.Fetch.Default = 1024 * 1024

	consumer, err := sarama.NewConsumerGroup([]string{brokers}, groupID, config)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer group: %w", err)
	}

	return &Consumer{
		consumer:      consumer,
		topic:         topic,
		groupID:       groupID,
		insertMessage: insertFn,
	}, nil
}

func (c *Consumer) Start() {
	go func() {
		for {
			handler := &consumerGroupHandler{insertMessage: c.insertMessage}
			err := c.consumer.Consume(nil, []string{c.topic}, handler)
			if err != nil {
				log.Printf("kafka consumer error: %v", err)
				time.Sleep(2 * time.Second)
			}
		}
	}()

	log.Printf("kafka consumer [%s] started on topic %s", c.groupID, c.topic)
}

func (c *Consumer) Close() error {
	return c.consumer.Close()
}

type consumerGroupHandler struct {
	insertMessage InsertMessageFunc
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var payload MessagePayload
		if err := json.Unmarshal(msg.Value, &payload); err != nil {
			log.Printf("kafka: failed to unmarshal message: %v", err)
			session.MarkMessage(msg, "")
			continue
		}

		var replyToJSON *string
		if payload.ReplyTo != nil {
			b, _ := json.Marshal(payload.ReplyTo)
			s := string(b)
			replyToJSON = &s
		}

		var forwardFromJSON *string
		if payload.ForwardFrom != nil {
			b, _ := json.Marshal(payload.ForwardFrom)
			s := string(b)
			forwardFromJSON = &s
		}

		if h.insertMessage != nil {
			h.insertMessage(
				payload.ID,
				payload.Content,
				payload.Type,
				payload.UniqId,
				payload.RoomID,
				payload.Username,
				payload.ClientID,
				payload.Deliver,
				payload.Read,
				replyToJSON,
				forwardFromJSON,
				payload.CreatedAt,
				payload.UpdatedAt,
			)
		}

		session.MarkMessage(msg, "")
	}
	return nil
}

func SetupConsumer(db interface{ InsertMessage(msgId string, content string, msgType string, uniqId string, roomID string, username string, clientID string, deliver []string, read []string, replyToJSON *string, forwardFromJSON *string, createdAt time.Time, updatedAt time.Time) }) (*Consumer, error) {
	kafkaEnabled := os.Getenv("KAFKA_ENABLED")
	if kafkaEnabled == "" || kafkaEnabled == "false" {
		log.Println("Kafka consumer disabled (KAFKA_ENABLED != true)")
		return nil, nil
	}
	return NewConsumer(db.InsertMessage)
}
