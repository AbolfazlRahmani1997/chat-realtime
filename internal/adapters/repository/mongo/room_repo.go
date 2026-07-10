package mongo

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"server/internal/core/domain"
)

type roomRepo struct {
	db *mongo.Database
}

func NewRoomRepo(db *mongo.Database) *roomRepo {
	return &roomRepo{db: db}
}

func (r *roomRepo) GetByID(id string) (*domain.Room, error) {
	var doc bson.M
	filter := bson.M{"id": id}
	err := r.db.Collection("rooms").FindOne(context.TODO(), filter).Decode(&doc)
	if err != nil {
		return nil, err
	}
	return bsonToRoom(doc), nil
}

func (r *roomRepo) GetUserRooms(userID string, page, offset int, filterType string) ([]domain.Room, error) {
	match := bson.M{
		"members": bson.M{
			"$elemMatch": bson.M{"id": userID, "pin": false},
		},
	}
	if filterType != "" {
		match["type"] = filterType
	}

	skip := int64((page * offset) - offset)
	limit := int64(offset)

	pipeline := bson.A{
		bson.M{"$match": match},
		bson.M{"$sort": bson.M{"last_message.created_at": -1}},
		bson.M{"$skip": skip},
		bson.M{"$limit": limit},
	}

	cursor, err := r.db.Collection("rooms").Aggregate(context.TODO(), pipeline)
	if err != nil {
		return nil, err
	}
	var docs []bson.M
	if err := cursor.All(context.TODO(), &docs); err != nil {
		return nil, err
	}
	return bsonToRooms(docs), nil
}

func (r *roomRepo) GetPinnedRooms(userID string, limit int) ([]domain.Room, error) {
	pipeline := bson.A{
		bson.M{"$match": bson.M{
			"members": bson.M{
				"$elemMatch": bson.M{"id": userID, "pin": true},
			},
		}},
		bson.M{"$sort": bson.M{"last_message.created_at": -1}},
		bson.M{"$limit": limit},
	}

	cursor, err := r.db.Collection("rooms").Aggregate(context.TODO(), pipeline)
	if err != nil {
		return nil, err
	}
	var docs []bson.M
	if err := cursor.All(context.TODO(), &docs); err != nil {
		return nil, err
	}
	return bsonToRooms(docs), nil
}

func (r *roomRepo) GetOnlineRooms(userID string) ([]domain.Room, error) {
	filter := bson.M{"members.id": userID}
	opts := options.Find().SetSort(bson.D{{Key: "last_message.created_at", Value: -1}})
	cursor, err := r.db.Collection("rooms").Find(context.TODO(), filter, opts)
	if err != nil {
		return nil, err
	}
	var docs []bson.M
	if err := cursor.All(context.TODO(), &docs); err != nil {
		return nil, err
	}
	return bsonToRooms(docs), nil
}

func (r *roomRepo) Save(room *domain.Room) error {
	doc := roomToBson(room)
	doc["created_at"] = time.Now()
	doc["updated_at"] = time.Now()
	_, err := r.db.Collection("rooms").InsertOne(context.TODO(), doc)
	return err
}

func (r *roomRepo) UpdateLastMessage(roomID string, msg *domain.Message) error {
	filter := bson.M{"id": roomID}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "last_message", Value: msg}}}}
	_, err := r.db.Collection("rooms").UpdateOne(context.TODO(), filter, update)
	return err
}

func (r *roomRepo) UpdateStatus(roomID string, status domain.MemberStatus) error {
	filter := bson.M{"id": roomID}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: string(status)}}}}
	err := r.db.Collection("rooms").FindOneAndUpdate(context.TODO(), filter, update).Decode(&bson.M{})
	return err
}

func (r *roomRepo) UpdateMembers(roomID string, members []domain.Member) error {
	filter := bson.M{"id": roomID}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "members", Value: members}}}}
	err := r.db.Collection("rooms").FindOneAndUpdate(context.TODO(), filter, update).Decode(&bson.M{})
	return err
}

func (r *roomRepo) UpdateRoom(room *domain.Room) error {
	filter := bson.M{"id": room.ID}
	updateFields := bson.D{}
	if room.Name != "" {
		updateFields = append(updateFields, bson.E{Key: "name", Value: room.Name})
	}
	if room.AvatarUrl != "" {
		updateFields = append(updateFields, bson.E{Key: "avatar_url", Value: room.AvatarUrl})
	}
	if len(room.Members) > 0 {
		updateFields = append(updateFields, bson.E{Key: "members", Value: room.Members})
	}
	if len(updateFields) == 0 {
		return nil
	}
	_, err := r.db.Collection("rooms").UpdateOne(context.TODO(), filter, bson.D{{Key: "$set", Value: updateFields}})
	return err
}

func (r *roomRepo) UpdateAvatar(roomID, avatarURL string) error {
	filter := bson.M{"id": roomID}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "avatar_url", Value: avatarURL}}}}
	_, err := r.db.Collection("rooms").UpdateOne(context.TODO(), filter, update)
	return err
}

func bsonToRoom(doc bson.M) *domain.Room {
	room := &domain.Room{
		ID:   toString(doc["id"]),
		Name: toString(doc["name"]),
	}
	if v, ok := doc["status"]; ok {
		room.Status = domain.MemberStatus(toString(v))
	}
	if v, ok := doc["type"]; ok {
		room.Type = domain.RoomType(toString(v))
	}
	if v, ok := doc["avatar_url"]; ok {
		room.AvatarUrl = toString(v)
	}
	if v, ok := doc["members"]; ok {
		if arr, ok := v.(bson.A); ok {
			for _, item := range arr {
				if m, ok := item.(bson.M); ok {
					room.Members = append(room.Members, bsonToMember(m))
				}
			}
		}
	}
	if v, ok := doc["last_message"]; ok {
		if m, ok := v.(bson.M); ok {
			msg := bsonToMessage(m)
			if msg != nil {
				room.Message = *msg
			}
		}
	}
	return room
}

func bsonToRooms(docs []bson.M) []domain.Room {
	result := make([]domain.Room, 0, len(docs))
	for _, doc := range docs {
		if r := bsonToRoom(doc); r != nil {
			result = append(result, *r)
		}
	}
	return result
}

func bsonToMember(doc bson.M) domain.Member {
	m := domain.Member{
		Id:        toString(doc["id"]),
		FirstName: toString(doc["firstname"]),
		LastName:  toString(doc["lastname"]),
		AvatarUrl: toString(doc["avatar_url"]),
	}
	if v, ok := doc["pin"]; ok {
		if b, ok := v.(bool); ok {
			m.Pin = b
		}
	}
	if v, ok := doc["Notification"]; ok {
		if b, ok := v.(bool); ok {
			m.Notification = b
		}
	}
	if v, ok := doc["roles"]; ok {
		if arr, ok := v.(bson.A); ok {
			for _, item := range arr {
				m.Roles = append(m.Roles, toString(item))
			}
		}
	}
	return m
}

func roomToBson(room *domain.Room) bson.M {
	doc := bson.M{
		"id":   room.ID,
		"name": room.Name,
		"type": string(room.Type),
	}
	members := make(bson.A, len(room.Members))
	for i, m := range room.Members {
		members[i] = bson.M{
			"id":           m.Id,
			"roles":        m.Roles,
			"firstname":    m.FirstName,
			"lastname":     m.LastName,
			"avatar_url":   m.AvatarUrl,
			"Notification": m.Notification,
			"pin":          m.Pin,
			"last_seen":    m.LastSeen,
		}
	}
	doc["members"] = members
	return doc
}
