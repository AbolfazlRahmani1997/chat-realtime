package mongo

import (
	"context"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"server/internal/core/domain"
)

type messageRepo struct {
	db *mongo.Database
}

func NewMessageRepo(db *mongo.Database) *messageRepo {
	return &messageRepo{db: db}
}

func (r *messageRepo) InsertMessage(msg *domain.Message) error {
	doc := bson.M{
		"_id":          ObjectIDFromHex(msg.UniqId),
		"content":      msg.Content,
		"message_type": msg.Type,
		"roomID":       msg.RoomID,
		"username":     msg.Username,
		"clientID":     msg.ClientID,
		"Deliver":      msg.Deliver,
		"Read":         msg.Read,
		"uniqid":       msg.UniqId,
		"created_at":   time.Now(),
		"updated_at":   time.Now(),
	}
	if msg.ReplyTo != nil {
		doc["reply_to"] = bson.M{
			"id":       msg.ReplyTo.ID,
			"content":  msg.ReplyTo.Content,
			"username": msg.ReplyTo.Username,
		}
	}
	if msg.ForwardFrom != nil {
		doc["forward_from"] = bson.M{
			"from_room_id": msg.ForwardFrom.FromRoomID,
			"from_msg_id":  msg.ForwardFrom.FromMsgID,
			"content":      msg.ForwardFrom.Content,
			"username":     msg.ForwardFrom.Username,
		}
	}
	_, err := r.db.Collection("messages").InsertOne(context.TODO(), doc)
	return err
}

func (r *messageRepo) GetMessageByID(id string) (*domain.Message, error) {
	var doc bson.M
	filter := bson.M{"_id": ObjectIDFromHex(id)}
	err := r.db.Collection("messages").FindOne(context.TODO(), filter).Decode(&doc)
	if err != nil {
		return nil, err
	}
	return bsonToMessage(doc), nil
}

func (r *messageRepo) GetMessageByUniqId(uniqId string) (*domain.Message, error) {
	var doc bson.M
	filter := bson.M{"uniqid": uniqId}
	err := r.db.Collection("messages").FindOne(context.TODO(), filter).Decode(&doc)
	if err != nil {
		return nil, err
	}
	return bsonToMessage(doc), nil
}

func (r *messageRepo) GetRoomMessages(roomID string, page, limit int) ([]domain.Message, error) {
	skip := int64((page - 1) * limit)
	l := int64(limit)
	opts := options.Find().SetSkip(skip).SetLimit(l).SetSort(bson.D{{Key: "created_at", Value: -1}})
	cursor, err := r.db.Collection("messages").Find(context.TODO(), bson.M{"roomID": roomID}, opts)
	if err != nil {
		return nil, err
	}
	var docs []bson.M
	if err := cursor.All(context.TODO(), &docs); err != nil {
		return nil, err
	}
	return bsonToMessages(docs), nil
}

func (r *messageRepo) GetUndeliveredMessages(roomID, userID string) ([]domain.Message, error) {
	filter := bson.M{
		"roomID":   roomID,
		"Deliver":  bson.M{"$eq": nil},
		"clientID": bson.M{"$ne": userID},
	}
	cursor, err := r.db.Collection("messages").Find(context.TODO(), filter)
	if err != nil {
		return nil, err
	}
	var docs []bson.M
	if err := cursor.All(context.TODO(), &docs); err != nil {
		return nil, err
	}
	return bsonToMessages(docs), nil
}

func (r *messageRepo) GetUndeliveredCount(roomID, userID string) (int64, error) {
	filter := bson.M{
		"roomID":   roomID,
		"Deliver":  bson.M{"$eq": nil},
		"clientID": bson.M{"$ne": userID},
	}
	return r.db.Collection("messages").CountDocuments(context.TODO(), filter)
}

func (r *messageRepo) UpdateDelivery(id string, clientIDs []string) error {
	filter := bson.M{"_id": ObjectIDFromHex(id)}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "Deliver", Value: clientIDs}}}}
	_, err := r.db.Collection("messages").UpdateOne(context.TODO(), filter, update)
	return err
}

func (r *messageRepo) UpdateReadReceipt(id string, clientIDs []string) error {
	filter := bson.M{"_id": ObjectIDFromHex(id)}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "Read", Value: clientIDs}}}}
	_, err := r.db.Collection("messages").UpdateOne(context.TODO(), filter, update)
	return err
}

func bsonToMessage(doc bson.M) *domain.Message {
	msg := &domain.Message{
		ID:       toString(doc["_id"]),
		Content:  toString(doc["content"]),
		Type:     toString(doc["message_type"]),
		RoomID:   toString(doc["roomID"]),
		Username: toString(doc["username"]),
		ClientID: toString(doc["clientID"]),
		UniqId:   toString(doc["uniqid"]),
	}
	if v, ok := doc["reply_to"]; ok {
		if rt, ok := v.(bson.M); ok {
			msg.ReplyTo = &domain.ReplyInfo{
				ID:       toString(rt["id"]),
				Content:  toString(rt["content"]),
				Username: toString(rt["username"]),
			}
		}
	}
	if v, ok := doc["forward_from"]; ok {
		if ff, ok := v.(bson.M); ok {
			msg.ForwardFrom = &domain.ForwardInfo{
				FromRoomID: toString(ff["from_room_id"]),
				FromMsgID:  toString(ff["from_msg_id"]),
				Content:    toString(ff["content"]),
				Username:   toString(ff["username"]),
			}
		}
	}
	if v, ok := doc["created_at"]; ok {
		if t, ok := v.(time.Time); ok {
			msg.CreatedAt = t
		}
	}
	if v, ok := doc["Deliver"]; ok {
		if arr, ok := v.(bson.A); ok {
			for _, item := range arr {
				msg.Deliver = append(msg.Deliver, toString(item))
			}
		}
	}
	if v, ok := doc["Read"]; ok {
		if arr, ok := v.(bson.A); ok {
			for _, item := range arr {
				msg.Read = append(msg.Read, toString(item))
			}
		}
	}
	return msg
}

func bsonToMessages(docs []bson.M) []domain.Message {
	result := make([]domain.Message, 0, len(docs))
	for _, doc := range docs {
		if m := bsonToMessage(doc); m != nil {
			result = append(result, *m)
		}
	}
	return result
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case int, int32, int64:
		return strconv.FormatInt(toInt64(val), 10)
	default:
		return ""
	}
}

func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	default:
		return 0
	}
}
