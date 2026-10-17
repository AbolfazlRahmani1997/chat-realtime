package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	RedisKeyPresence       = "chat:presence"
	RedisKeyPresenceCounts = "chat:presence:counts"
	RedisKeyOnlineUser     = "chat:online:%s"
	RedisKeyActiveRooms    = "chat:active_rooms"
	RedisKeyRoomClients    = "chat:room:%s:clients"
	RedisKeyUserRooms      = "chat:user:%s:rooms"
	RedisKeyInstanceUser   = "chat:instance:%s:users"

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
	return &RedisPresenceManager{
		client:     client,
		ctx:        context.Background(),
		instanceID: instanceIdentity(),
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
	pipe.HIncrBy(p.ctx, RedisKeyPresenceCounts, userID, 1)
	pipe.Set(p.ctx, fmt.Sprintf(RedisKeyOnlineUser, userID), "1", PresenceTTL)
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
	n, err := p.client.HIncrBy(p.ctx, RedisKeyPresenceCounts, userID, -1).Result()
	if err != nil {
		log.Printf("redis presence: failed to decrement user connection: %v", err)
	}
	if n <= 0 {
		pipe.HDel(p.ctx, RedisKeyPresenceCounts, userID)
		pipe.Del(p.ctx, fmt.Sprintf(RedisKeyOnlineUser, userID))
		pipe.SRem(p.ctx, fmt.Sprintf(RedisKeyInstanceUser, p.instanceID), userID)
	}
	_, err = pipe.Exec(p.ctx)
	if err != nil {
		log.Printf("redis presence: failed to remove user connection: %v", err)
	}
}

func (p *RedisPresenceManager) IsUserOnline(userID string) bool {
	n, err := p.client.Exists(p.ctx, fmt.Sprintf(RedisKeyOnlineUser, userID)).Result()
	return err == nil && n == 1
}

func (p *RedisPresenceManager) BroadcastUserOnline(userID string, rooms []Room) {
	for _, room := range rooms {
		p.broker.PublishRoomEvent(&PubSubRoomEvent{
			Type:   "user_online",
			RoomID: room.ID,
			UserID: userID,
			Status: "online",
		})
	}
}

func (p *RedisPresenceManager) BroadcastUserOffline(userID string, rooms []Room) {
	for _, room := range rooms {
		p.broker.PublishRoomEvent(&PubSubRoomEvent{
			Type:   "user_offline",
			RoomID: room.ID,
			UserID: userID,
			Status: "offline",
		})
	}
}
