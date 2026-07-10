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
	RedisKeyPresence     = "chat:presence"
	RedisKeyActiveRooms  = "chat:active_rooms"
	RedisKeyRoomClients  = "chat:room:%s:clients"
	RedisKeyUserRooms    = "chat:user:%s:rooms"
	RedisKeyInstanceUser = "chat:instance:%s:users"

	PresenceTTL = 2 * time.Minute
)

type PresenceEntry struct {
	UserID       string    `json:"user_id"`
	InstanceID   string    `json:"instance_id"`
	ConnectionID string    `json:"connection_id"`
	ConnectedAt  time.Time `json:"connected_at"`
	Online       bool      `json:"online"`
}

type RedisPresenceManager struct {
	client     *redis.Client
	ctx        context.Context
	instanceID string
	hub        *Hub
	broker     *RedisPubSubBroker
}

func NewRedisPresenceManager(hub *Hub, broker *RedisPubSubBroker, client *redis.Client) *RedisPresenceManager {
	instanceID := fmt.Sprintf("instance-%d", time.Now().UnixNano())
	if hostname, err := os.Hostname(); err == nil {
		instanceID = hostname
	}

	return &RedisPresenceManager{
		client:     client,
		ctx:        context.Background(),
		instanceID: instanceID,
		hub:        hub,
		broker:     broker,
	}
}

func (p *RedisPresenceManager) AddUserConnection(userID string, connectionID string) {
	entry := PresenceEntry{
		UserID:       userID,
		InstanceID:   p.instanceID,
		ConnectionID: connectionID,
		ConnectedAt:  time.Now(),
		Online:       true,
	}
	data, _ := json.Marshal(entry)

	pipe := p.client.Pipeline()
	pipe.HSet(p.ctx, RedisKeyPresence, fmt.Sprintf("%s:%s", userID, connectionID), string(data))
	pipe.Expire(p.ctx, RedisKeyPresence, PresenceTTL*10)
	pipe.SAdd(p.ctx, fmt.Sprintf(RedisKeyInstanceUser, p.instanceID), userID)
	pipe.Expire(p.ctx, fmt.Sprintf(RedisKeyInstanceUser, p.instanceID), PresenceTTL*10)
	_, err := pipe.Exec(p.ctx)
	if err != nil {
		log.Printf("redis presence: failed to add user connection: %v", err)
	}
}

func (p *RedisPresenceManager) RemoveUserConnection(userID string, connectionID string) {
	pipe := p.client.Pipeline()
	pipe.HDel(p.ctx, RedisKeyPresence, fmt.Sprintf("%s:%s", userID, connectionID))

	count, _ := p.client.HLen(p.ctx, RedisKeyPresence).Result()
	if count == 0 {
		pipe.SRem(p.ctx, fmt.Sprintf(RedisKeyInstanceUser, p.instanceID), userID)
	}
	_, err := pipe.Exec(p.ctx)
	if err != nil {
		log.Printf("redis presence: failed to remove user connection: %v", err)
	}
}

func (p *RedisPresenceManager) IsUserOnline(userID string) bool {
	keys, err := p.client.HKeys(p.ctx, RedisKeyPresence).Result()
	if err != nil {
		return false
	}
	for _, key := range keys {
		var entry PresenceEntry
		val, _ := p.client.HGet(p.ctx, RedisKeyPresence, key).Result()
		if err := json.Unmarshal([]byte(val), &entry); err == nil && entry.UserID == userID {
			return true
		}
	}
	return false
}

func (p *RedisPresenceManager) BroadcastUserOnline(userID string, rooms []Room) {
	for _, room := range rooms {
		for _, member := range room.Members {
			if member.Id == userID {
				continue
			}
			p.broker.PublishRoomEvent(&PubSubRoomEvent{
				Type:   "user_online",
				RoomID: room.ID,
				UserID: userID,
				Status: "online",
			})
		}
	}
}

func (p *RedisPresenceManager) BroadcastUserOffline(userID string, rooms []Room) {
	for _, room := range rooms {
		for _, member := range room.Members {
			if member.Id == userID {
				continue
			}
			p.broker.PublishRoomEvent(&PubSubRoomEvent{
				Type:   "user_offline",
				RoomID: room.ID,
				UserID: userID,
				Status: "offline",
			})
		}
	}
}
