package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	PubSubChannelMessages    = "chat:messages"
	PubSubChannelRoomEvents  = "chat:room_events"
	PubSubChannelPresence    = "chat:presence"
	PubSubChannelReadReceipt = "chat:read_receipt"
)

type PubSubMessage struct {
	Type      string   `json:"type"`
	RoomID    string   `json:"room_id,omitempty"`
	UserID    string   `json:"user_id,omitempty"`
	Message   *Message `json:"message,omitempty"`
	Timestamp int64    `json:"timestamp"`
	SenderID  string   `json:"sender_id,omitempty"`
}

type PubSubRoomEvent struct {
	Type     string `json:"type"`
	RoomID   string `json:"room_id"`
	UserID   string `json:"user_id,omitempty"`
	Status   string `json:"status,omitempty"`
	RoomData *Room  `json:"room_data,omitempty"`
	SenderID string `json:"sender_id,omitempty"`
}

type PubSubReadReceipt struct {
	MessageID string `json:"message_id"`
	UserID    string `json:"user_id"`
	RoomID    string `json:"room_id"`
}

type RedisPubSubBroker struct {
	client     *redis.Client
	pubsub     *redis.PubSub
	ctx        context.Context
	instanceID string
	hub        *Hub
}

func NewRedisPubSubBroker(hub *Hub, client *redis.Client) *RedisPubSubBroker {
	instanceID := fmt.Sprintf("instance-%d", time.Now().UnixNano())
	if hostname, err := os.Hostname(); err == nil {
		instanceID = hostname
	}

	return &RedisPubSubBroker{
		client:     client,
		ctx:        context.Background(),
		instanceID: instanceID,
		hub:        hub,
	}
}

func (b *RedisPubSubBroker) Start() {
	log.Printf("[redis-pubsub] starting redis pubsub with instanceID=%s", b.instanceID)
	b.pubsub = b.client.Subscribe(b.ctx, PubSubChannelMessages, PubSubChannelRoomEvents, PubSubChannelReadReceipt)
	ch := b.pubsub.Channel()

	go func() {
		for msg := range ch {
			switch msg.Channel {
			case PubSubChannelMessages:
				var pubMsg PubSubMessage
				if err := json.Unmarshal([]byte(msg.Payload), &pubMsg); err != nil {
					log.Printf("redis pubsub: failed to unmarshal message: %v", err)
					continue
				}
				if pubMsg.SenderID == b.instanceID {
					continue
				}
				b.handleIncomingMessage(&pubMsg)

			case PubSubChannelRoomEvents:
				var event PubSubRoomEvent
				if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
					log.Printf("redis pubsub: failed to unmarshal room event: %v", err)
					continue
				}
				b.handleIncomingRoomEvent(&event)

			case PubSubChannelReadReceipt:
				var receipt PubSubReadReceipt
				if err := json.Unmarshal([]byte(msg.Payload), &receipt); err != nil {
					log.Printf("redis pubsub: failed to unmarshal read receipt: %v", err)
					continue
				}
				b.handleIncomingReadReceipt(&receipt)
			}
		}
	}()
}

func (b *RedisPubSubBroker) Close() {
	if b.pubsub != nil {
		_ = b.pubsub.Close()
	}
}

func (b *RedisPubSubBroker) PublishMessage(roomID string, m *Message) {
	pubMsg := PubSubMessage{
		Type:      "new_message",
		RoomID:    roomID,
		Message:   m,
		Timestamp: time.Now().Unix(),
		SenderID:  b.instanceID,
	}
	data, _ := json.Marshal(pubMsg)
	err := b.client.Publish(b.ctx, PubSubChannelMessages, string(data)).Err()
	if err != nil {
		log.Printf("redis pubsub: failed to publish message: %v", err)
	}
}

func (b *RedisPubSubBroker) PublishRoomEvent(event *PubSubRoomEvent) {
	event.SenderID = b.instanceID
	data, _ := json.Marshal(event)
	err := b.client.Publish(b.ctx, PubSubChannelRoomEvents, string(data)).Err()
	if err != nil {
		log.Printf("redis pubsub: failed to publish room event: %v", err)
	}
}

func (b *RedisPubSubBroker) PublishReadReceipt(receipt *PubSubReadReceipt) {
	data, _ := json.Marshal(receipt)
	err := b.client.Publish(b.ctx, PubSubChannelReadReceipt, string(data)).Err()
	if err != nil {
		log.Printf("redis pubsub: failed to publish read receipt: %v", err)
	}
}

func (b *RedisPubSubBroker) handleIncomingMessage(pubMsg *PubSubMessage) {
	if pubMsg.Message == nil || b.hub == nil {
		return
	}
	m := pubMsg.Message
	msg := &Message{
		ID:          m.ID,
		Content:     m.Content,
		Type:        m.Type,
		UniqId:      m.UniqId,
		RoomID:      m.RoomID,
		Username:    m.Username,
		ClientID:    m.ClientID,
		Deliver:     m.Deliver,
		Read:        m.Read,
		ReplyTo:     m.ReplyTo,
		ForwardFrom: m.ForwardFrom,
	}
	b.hub.Broadcast <- msg
}

func (b *RedisPubSubBroker) handleIncomingRoomEvent(event *PubSubRoomEvent) {
	if b.hub == nil {
		return
	}
	switch event.Type {
	case "room_created":
		if event.RoomData != nil {
			b.hub.mx.Lock()
			if _, exists := b.hub.Rooms[event.RoomData.ID]; !exists {
				room := &Room{
					ID:      event.RoomData.ID,
					Name:    event.RoomData.Name,
					Type:    event.RoomData.Type,
					Members: event.RoomData.Members,
					Status:  event.RoomData.Status,
					Clients: make(map[string]*Client),
				}
				b.hub.Rooms[event.RoomData.ID] = room
			}
			b.hub.mx.Unlock()
		}

	case "user_online":
		b.hub.mx.RLock()
		user, exists := b.hub.Users[event.UserID]
		b.hub.mx.RUnlock()

		if exists && user != nil {
			select {
			case user.roomStatuses <- &RoomStatus{RoomId: event.RoomID, Status: online}:
			default:
			}
		}

	case "user_offline":
		b.hub.mx.RLock()
		user, exists := b.hub.Users[event.UserID]
		b.hub.mx.RUnlock()

		if exists && user != nil {
			select {
			case user.roomStatuses <- &RoomStatus{RoomId: event.RoomID, Status: offline}:
			default:
			}
		}
	}
}

func (b *RedisPubSubBroker) handleIncomingReadReceipt(receipt *PubSubReadReceipt) {
	if b.hub == nil {
		return
	}
	b.hub.mx.RLock()
	user, exists := b.hub.Users[receipt.UserID]
	b.hub.mx.RUnlock()

	if exists && user != nil {
		select {
		case user.seenMessage <- &SeenNotification{MessageId: receipt.MessageID, RoomId: receipt.RoomID}:
		default:
		}
	}
}
