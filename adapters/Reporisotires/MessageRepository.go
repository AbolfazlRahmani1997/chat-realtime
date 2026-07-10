package Reporisotires

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/net/context"
	"os"
	"server/Dtos/DtoReporitories"
	"server/entity/Message"
)

type MessageRepository struct {
	Collection *mongo.Database
}

func NewMessageRepository(client *mongo.Client) *MessageRepository {
	dbName := os.Getenv("CHAT_DB")
	if dbName == "" {
		dbName = "chat_db"
	}
	return &MessageRepository{client.Database(dbName)}
}

func (r *MessageRepository) GetMessage(id string) Message.Message {
	var message Message.Message
	if id == "" {
		return Message.Message{}
	}

	// Try Mongo ObjectID first, otherwise fall back to ULID/uniqid (used by ws "seenMessage").
	condition := bson.M{}
	if oid, err := primitive.ObjectIDFromHex(id); err == nil {
		condition = bson.M{"_id": oid}
	} else {
		condition = bson.M{"uniqid": id}
	}

	find := r.Collection.Collection("messages").FindOne(context.TODO(), condition)
	if err := find.Decode(&message); err != nil {
		return Message.Message{}
	}
	return message
}

func (r *MessageRepository) GetRoomMessages(page int, offset int, filter DtoReporitories.MessageFilterDto) []Message.Message {
	if page < 1 {
		page = 1
	}
	if offset <= 0 {
		offset = 10
	}

	skip := int64((page - 1) * offset)
	limit := int64(offset)

	opts := options.Find().
		SetSkip(skip).
		SetLimit(limit).
		SetSort(bson.D{{Key: "created_at", Value: -1}})

	findFilter := r.queryBuilding(filter)
	cur, err := r.Collection.Collection("messages").Find(context.TODO(), findFilter, opts)
	if err != nil {
		return []Message.Message{}
	}

	var messages []Message.Message
	if err := cur.All(context.TODO(), &messages); err != nil {
		return []Message.Message{}
	}
	return messages
}

func (r *MessageRepository) queryBuilding(filter DtoReporitories.MessageFilterDto) bson.M {
	query := bson.M{}
	if filter.RoomId != "" {
		// Match schema used by internal/ws (RoomID -> "roomID")
		query["roomID"] = filter.RoomId
	}
	if filter.ClientId != "" {
		// Match schema used by internal/ws (ClientID -> "clientID")
		query["clientID"] = filter.ClientId
	}

	return query
}
