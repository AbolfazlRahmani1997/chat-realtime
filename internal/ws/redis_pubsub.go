package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
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
	Type      string     `json:"type"`
	RoomID    string     `json:"room_id"`
	UserID    string     `json:"user_id,omitempty"`
	Status    string     `json:"status,omitempty"`
	RoomData  *Room      `json:"room_data,omitempty"`
	SenderID  string     `json:"sender_id,omitempty"`
	MessageID string     `json:"message_id,omitempty"`
	Reactions []Reaction `json:"reactions,omitempty"`
	Body      string     `json:"body,omitempty"`
}

type PubSubReadReceipt struct {
	MessageID string `json:"message_id"`
	UserID    string `json:"user_id"`             // reader
	AuthorID  string `json:"author_id,omitempty"` // message author to notify
	RoomID    string `json:"room_id"`
}

type RedisPubSubBroker struct {
	client     *redis.Client
	pubsub     *redis.PubSub
	ctx        context.Context
	instanceID string
	hub        *Hub
	subMu      sync.Mutex
	msgSubs    map[string]bool
}

func NewRedisPubSubBroker(hub *Hub, client *redis.Client) *RedisPubSubBroker {
	return &RedisPubSubBroker{
		client:     client,
		ctx:        context.Background(),
		instanceID: instanceIdentity(),
		hub:        hub,
		msgSubs:    make(map[string]bool),
	}
}

func instanceIdentity() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "local"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

func (b *RedisPubSubBroker) Start() {
	log.Printf("[redis-pubsub] starting redis pubsub with instanceID=%s", b.instanceID)
	// Message traffic is sharded. This pod subscribes to a shard only after a
	// local user watches a room on it, so it does not decode the whole cluster.
	b.pubsub = b.client.Subscribe(b.ctx, PubSubChannelRoomEvents, PubSubChannelReadReceipt)
	ch := b.pubsub.Channel(redis.WithChannelSize(8192))

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

func (b *RedisPubSubBroker) SubscribeMessages(channels []string) {
	if b == nil || b.pubsub == nil || len(channels) == 0 {
		return
	}
	b.subMu.Lock()
	defer b.subMu.Unlock()
	var fresh []string
	for _, ch := range channels {
		if ch == "" || b.msgSubs[ch] {
			continue
		}
		b.msgSubs[ch] = true
		fresh = append(fresh, ch)
	}
	if len(fresh) == 0 {
		return
	}
	if err := b.pubsub.Subscribe(b.ctx, fresh...); err != nil {
		log.Printf("redis pubsub: subscribe %v: %v", fresh, err)
	}
}

func (b *RedisPubSubBroker) UnsubscribeMessages(channels []string) {
	if b == nil || b.pubsub == nil || len(channels) == 0 {
		return
	}
	b.subMu.Lock()
	defer b.subMu.Unlock()
	var drop []string
	for _, ch := range channels {
		if ch == "" || !b.msgSubs[ch] {
			continue
		}
		delete(b.msgSubs, ch)
		drop = append(drop, ch)
	}
	if len(drop) == 0 {
		return
	}
	if err := b.pubsub.Unsubscribe(b.ctx, drop...); err != nil {
		log.Printf("redis pubsub: unsubscribe %v: %v", drop, err)
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
	err := b.client.Publish(b.ctx, messageChannel(roomID), data).Err()
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
		remote:      true,
		CreatedAt:   m.CreatedAt,
		UpdatedAt:   m.UpdatedAt,
	}
	b.hub.EnqueueBroadcast(msg)
}

func (b *RedisPubSubBroker) handleIncomingRoomEvent(event *PubSubRoomEvent) {
	if b.hub == nil || event == nil {
		return
	}
	if event.SenderID != "" && event.SenderID == b.instanceID {
		return
	}
	switch event.Type {
	case "room_created":
		if event.RoomData == nil {
			return
		}
		if b.hub.hasRoomWatchers(event.RoomID) {
			b.hub.EnsureRoom(*event.RoomData)
		}
		for _, user := range b.hub.watchersExcept(event.RoomID, "") {
			user.trySendCreateRoom(event.RoomData)
		}

	case "user_online":
		for _, user := range b.hub.watchersExcept(event.RoomID, event.UserID) {
			user.trySendRoomStatus(&RoomStatus{RoomId: event.RoomID, Status: online})
		}

	case "user_offline":
		for _, user := range b.hub.watchersExcept(event.RoomID, event.UserID) {
			user.trySendRoomStatus(&RoomStatus{RoomId: event.RoomID, Status: offline})
		}

	case "message_reaction":
		reactions := event.Reactions
		if reactions == nil {
			reactions = []Reaction{}
		}
		b.hub.fanoutReaction(event.RoomID, event.MessageID, reactions)

	case "message_edited":
		b.hub.fanoutEdit(event.RoomID, event.MessageID, event.Body)

	case "secret_viewed":
		b.hub.fanoutSecret(event.RoomID, "secretViewed", event.MessageID, event.UserID)

	case "secret_purged":
		b.hub.fanoutSecret(event.RoomID, "secretPurged", event.MessageID, event.UserID)
	}
}

func (b *RedisPubSubBroker) handleIncomingReadReceipt(receipt *PubSubReadReceipt) {
	if b.hub == nil {
		return
	}
	// Notify the author on this instance; fall back only if AuthorID missing (legacy payloads).
	targetID := receipt.AuthorID
	if targetID == "" || targetID == receipt.UserID {
		return
	}
	if user, ok := b.hub.getUserLocked(targetID); ok && user != nil {
		user.trySendSeen(&SeenNotification{MessageId: receipt.MessageID, RoomId: receipt.RoomID})
	}
}
