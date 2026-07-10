package ws

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoProvider struct {
	db *mongo.Database
}

func NewMongoProvider(client *mongo.Database) *MongoProvider {
	return &MongoProvider{db: client}
}

func (p *MongoProvider) MessageOps() MessageOps { return p }
func (p *MongoProvider) RoomOps() RoomOps       { return p }
func (p *MongoProvider) UserOps() UserOps       { return p }

func getDbName() string {
	dbName := os.Getenv("CHAT_DB")
	if dbName == "" {
		dbName = "chat_db"
	}
	return dbName
}

// --- MessageOps ---

func (p *MongoProvider) InsertMessage(message Message) {
	p.InsertMessageFlat(message.ID, message.Content, message.Type, message.UniqId, message.RoomID, message.Username, message.ClientID, message.Deliver, message.Read, nil, nil, message.CreatedAt, message.UpdatedAt)
}

func (p *MongoProvider) InsertMessageFlat(msgId string, content string, msgType string, uniqId string, roomID string, username string, clientID string, deliver []string, read []string, replyToJSON *string, forwardFromJSON *string, createdAt time.Time, updatedAt time.Time) {
	now := time.Now()
	if createdAt.IsZero() {
		createdAt = now
	}
	if updatedAt.IsZero() {
		updatedAt = now
	}
	id := msgId
	if id == "" {
		id = uniqId
	}
	if id == "" {
		id = newULID()
	}

	delivered := len(deliver) > 0
	isRead := len(read) > 0

	doc := bson.M{
		"_id":          id,
		"content":      content,
		"message_type": msgType,
		"roomID":       roomID,
		"username":     username,
		"clientID":     clientID,
		"uniqid":       uniqId,
		"delivered":    delivered,
		"is_read":      isRead,
		"created_at":   createdAt,
		"updated_at":   updatedAt,
	}
	if replyToJSON != nil {
		var rt map[string]interface{}
		if json.Unmarshal([]byte(*replyToJSON), &rt) == nil {
			doc["reply_to"] = rt
		}
	}
	if forwardFromJSON != nil {
		var ff map[string]interface{}
		if json.Unmarshal([]byte(*forwardFromJSON), &ff) == nil {
			doc["forward_from"] = ff
		}
	}
	p.db.Collection("messages").InsertOne(context.TODO(), doc)

	roomDoc := bson.M{
		"_id":          id,
		"room_id":      roomID,
		"created_at":   createdAt,
		"content":      content,
		"message_type": msgType,
		"username":     username,
		"client_id":    clientID,
		"uniqid":       uniqId,
		"delivered":    delivered,
		"is_read":      isRead,
	}
	if replyToJSON != nil {
		var rt map[string]interface{}
		if json.Unmarshal([]byte(*replyToJSON), &rt) == nil {
			roomDoc["reply_to"] = rt
		}
	}
	if forwardFromJSON != nil {
		var ff map[string]interface{}
		if json.Unmarshal([]byte(*forwardFromJSON), &ff) == nil {
			roomDoc["forward_from"] = ff
		}
	}
	if _, err := p.db.Collection("messages_by_room").InsertOne(context.TODO(), roomDoc); err != nil {
		log.Printf("InsertMessageFlat: InsertOne error for messages_by_room (msg %s): %v", id, err)
	}
}

func msgFromDoc(doc bson.M) Message {
	msg := Message{}
	if v, ok := doc["_id"]; ok {
		msg.ID = toString(v)
	}
	if v, ok := doc["content"]; ok {
		msg.Content = toString(v)
	}
	if v, ok := doc["message_type"]; ok {
		msg.Type = toString(v)
	}
	if v, ok := doc["roomID"]; ok {
		msg.RoomID = toString(v)
	} else if v, ok := doc["room_id"]; ok {
		msg.RoomID = toString(v)
	}
	if v, ok := doc["username"]; ok {
		msg.Username = toString(v)
	}
	if v, ok := doc["clientID"]; ok {
		msg.ClientID = toString(v)
	} else if v, ok := doc["client_id"]; ok {
		msg.ClientID = toString(v)
	}
	if v, ok := doc["uniqid"]; ok {
		msg.UniqId = toString(v)
	}
	if v, ok := doc["reply_to"]; ok {
		if rt, ok := v.(bson.M); ok {
			replyInfo := &ReplyInfo{}
			if id, ok := rt["id"]; ok {
				replyInfo.ID = toString(id)
			}
			if content, ok := rt["content"]; ok {
				replyInfo.Content = toString(content)
			}
			if username, ok := rt["username"]; ok {
				replyInfo.Username = toString(username)
			}
			msg.ReplyTo = replyInfo
		}
	}
	if v, ok := doc["forward_from"]; ok {
		if ff, ok := v.(bson.M); ok {
			forwardInfo := &ForwardInfo{}
			if v, ok := ff["from_room_id"]; ok {
				forwardInfo.FromRoomID = toString(v)
			}
			if v, ok := ff["from_msg_id"]; ok {
				forwardInfo.FromMsgID = toString(v)
			}
			if v, ok := ff["content"]; ok {
				forwardInfo.Content = toString(v)
			}
			if v, ok := ff["username"]; ok {
				forwardInfo.Username = toString(v)
			}
			msg.ForwardFrom = forwardInfo
		}
	}
	if v, ok := doc["created_at"]; ok {
		if t, ok := v.(time.Time); ok {
			msg.CreatedAt = t
		}
	}
	if v, ok := doc["updated_at"]; ok {
		if t, ok := v.(time.Time); ok {
			msg.UpdatedAt = t
		}
	}
	return msg
}

func (p *MongoProvider) GetMessageNotDelivery(roomId, userId string) []Message {
	filter := bson.M{
		"roomID":    roomId,
		"delivered": bson.M{"$ne": true},
		"clientID":  bson.M{"$ne": userId},
	}
	cursor, err := p.db.Collection("messages").Find(context.TODO(), filter)
	if err != nil || cursor == nil {
		log.Printf("GetMessageNotDelivery: Find error for room %s, user %s: %v", roomId, userId, err)
		return nil
	}
	var docs []bson.M
	if err := cursor.All(context.TODO(), &docs); err != nil {
		log.Printf("GetMessageNotDelivery: cursor.All error: %v", err)
		return nil
	}
	result := make([]Message, 0, len(docs))
	for _, doc := range docs {
		result = append(result, msgFromDoc(doc))
	}
	return result
}

func (p *MongoProvider) GetRoomMessages(roomId, userId string) []Message {
	filter := bson.M{"roomID": roomId}
	opts := options.Find().SetLimit(1)
	cursor, err := p.db.Collection("messages").Find(context.TODO(), filter, opts)
	if err != nil || cursor == nil {
		log.Printf("GetRoomMessages: Find error for room %s, user %s: %v", roomId, userId, err)
		return nil
	}
	var docs []bson.M
	if err := cursor.All(context.TODO(), &docs); err != nil {
		log.Printf("GetRoomMessages: cursor.All error: %v", err)
		return nil
	}
	result := make([]Message, 0, len(docs))
	for _, doc := range docs {
		result = append(result, msgFromDoc(doc))
	}
	return result
}

func (p *MongoProvider) GetMessageNotCountDelivery(roomId, userId string) int64 {
	filter := bson.M{
		"roomID":    roomId,
		"delivered": bson.M{"$ne": true},
		"clientID":  bson.M{"$ne": userId},
	}
	count, _ := p.db.Collection("messages").CountDocuments(context.TODO(), filter)
	return count
}

func (p *MongoProvider) GetRoomMessage(roomId, page string) []Message {
	limit := int64(10)
	pageInt, _ := strconv.Atoi(page)
	if pageInt < 1 {
		pageInt = 1
	}
	skip := int64(pageInt-1) * limit
	opts := options.Find().SetSkip(skip).SetLimit(limit).SetSort(bson.D{{Key: "created_at", Value: -1}})
	cursor, err := p.db.Collection("messages_by_room").Find(context.TODO(), bson.M{"room_id": roomId}, opts)
	if err != nil || cursor == nil {
		return nil
	}
	var docs []bson.M
	cursor.All(context.TODO(), &docs)
	result := make([]Message, 0, len(docs))
	for _, doc := range docs {
		result = append(result, msgFromDoc(doc))
	}
	return result
}

func (p *MongoProvider) GetAllMessages(roomId, userId string) []Message {
	filter := bson.M{"roomID": roomId}
	opts := options.Find().SetLimit(10)
	cursor, err := p.db.Collection("messages").Find(context.TODO(), filter, opts)
	if err != nil || cursor == nil {
		return nil
	}
	var docs []bson.M
	cursor.All(context.TODO(), &docs)
	result := make([]Message, 0, len(docs))
	for _, doc := range docs {
		result = append(result, msgFromDoc(doc))
	}
	return result
}

func (p *MongoProvider) InsertRoom(room Room) {
	members := make([]bson.M, len(room.Members))
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
	doc := bson.M{
		"id":         room.ID,
		"name":       room.Name,
		"type":       string(room.Type),
		"members":    members,
		"status":     string(room.Status),
		"created_at": time.Now(),
		"updated_at": time.Now(),
	}
	if _, err := p.db.Collection("rooms").InsertOne(context.TODO(), doc); err != nil {
		log.Printf("InsertRoom: InsertOne error for room %s: %v", room.ID, err)
	}
}

func roomFromDoc(doc bson.M) Room {
	room := Room{}
	if v, ok := doc["id"]; ok {
		room.ID = toString(v)
	}
	if v, ok := doc["name"]; ok {
		room.Name = toString(v)
	}
	if v, ok := doc["type"]; ok {
		room.Type = Type(toString(v))
	}
	if v, ok := doc["status"]; ok {
		room.Status = status(toString(v))
	}
	if v, ok := doc["members"]; ok {
		if arr, ok := v.(bson.A); ok {
			for _, item := range arr {
				if m, ok := item.(bson.M); ok {
					member := Member{}
					if v, ok := m["id"]; ok {
						member.Id = toString(v)
					}
					if v, ok := m["firstname"]; ok {
						member.FirstName = toString(v)
					}
					if v, ok := m["lastname"]; ok {
						member.LastName = toString(v)
					}
					if v, ok := m["avatar_url"]; ok {
						member.AvatarUrl = toString(v)
					}
					if v, ok := m["Notification"]; ok {
						if b, ok := v.(bool); ok {
							member.Notification = b
						}
					}
					if v, ok := m["pin"]; ok {
						if b, ok := v.(bool); ok {
							member.Pin = b
						}
					}
					if v, ok := m["roles"]; ok {
						if arr, ok := v.(bson.A); ok {
							for _, item := range arr {
								member.Roles = append(member.Roles, toString(item))
							}
						}
					}
					room.Members = append(room.Members, member)
				}
			}
		}
	}
	return room
}

func (p *MongoProvider) GetRoom(roomId string) Room {
	var doc bson.M
	err := p.db.Collection("rooms").FindOne(context.TODO(), bson.M{"id": roomId}).Decode(&doc)
	if err != nil {
		log.Printf("GetRoom: FindOne error for room %s: %v", roomId, err)
		return Room{}
	}
	return roomFromDoc(doc)
}

func (p *MongoProvider) GetMessage(messageId string) Message {
	var doc bson.M
	err := p.db.Collection("messages").FindOne(context.TODO(), bson.M{"_id": messageId}).Decode(&doc)
	if err != nil {
		return Message{}
	}
	return msgFromDoc(doc)
}

func (p *MongoProvider) GetMessageById(id string) Message {
	return p.GetMessage(id)
}

func (p *MongoProvider) GetMessageByUniqId(uniqId string) Message {
	var doc bson.M
	err := p.db.Collection("messages").FindOne(context.TODO(), bson.M{"uniqid": uniqId}).Decode(&doc)
	if err != nil {
		return Message{}
	}
	return msgFromDoc(doc)
}

func (p *MongoProvider) MessageDelivery(id string, clientIds []string) error {
	_, err := p.db.Collection("messages").UpdateOne(context.TODO(), bson.M{"_id": id}, bson.D{{Key: "$set", Value: bson.D{{Key: "delivered", Value: true}}}})
	p.db.Collection("messages_by_room").UpdateOne(context.TODO(), bson.M{"_id": id}, bson.D{{Key: "$set", Value: bson.D{{Key: "delivered", Value: true}}}})
	return err
}

func (p *MongoProvider) MessageRead(id string, clientIds []string) error {
	_, err := p.db.Collection("messages").UpdateOne(context.TODO(), bson.M{"_id": id}, bson.D{{Key: "$set", Value: bson.D{{Key: "is_read", Value: true}}}})
	p.db.Collection("messages_by_room").UpdateOne(context.TODO(), bson.M{"_id": id}, bson.D{{Key: "$set", Value: bson.D{{Key: "is_read", Value: true}}}})
	return err
}

func (p *MongoProvider) UpdateRoomById(id string, room Room) {
	members := make([]bson.M, len(room.Members))
	for i, m := range room.Members {
		members[i] = bson.M{
			"id": m.Id, "roles": m.Roles, "firstname": m.FirstName,
			"lastname": m.LastName, "avatar_url": m.AvatarUrl,
			"Notification": m.Notification, "pin": m.Pin, "last_seen": m.LastSeen,
		}
	}
	update := bson.D{{Key: "$set", Value: bson.D{
		{Key: "status", Value: string(room.Status)},
		{Key: "members", Value: members},
		{Key: "updated_at", Value: time.Now()},
	}}}
	p.db.Collection("rooms").UpdateOne(context.TODO(), bson.M{"id": id}, update)
}

// --- RoomOps ---

func (p *MongoProvider) GetById(roomId string) Room {
	return p.GetRoom(roomId)
}

func (p *MongoProvider) GetMyRooms(userId string, page, offset int, filterType string) []Room {
	match := bson.M{"members": bson.M{"$elemMatch": bson.M{"id": userId}}}
	if filterType != "" {
		match["type"] = filterType
	}
	skip := int64((page * offset) - offset)
	l := int64(offset)
	pipeline := bson.A{
		bson.M{"$match": match},
		bson.M{"$sort": bson.M{"created_at": -1}},
		bson.M{"$skip": skip},
		bson.M{"$limit": l},
	}
	cursor, err := p.db.Collection("rooms").Aggregate(context.TODO(), pipeline)
	if err != nil || cursor == nil {
		log.Printf("GetMyRooms: Aggregate error for user %s: %v", userId, err)
		return nil
	}
	var docs []bson.M
	if err := cursor.All(context.TODO(), &docs); err != nil {
		log.Printf("GetMyRooms: cursor.All error for user %s: %v", userId, err)
		return nil
	}
	result := make([]Room, 0, len(docs))
	for _, doc := range docs {
		result = append(result, roomFromDoc(doc))
	}
	return result
}

func (p *MongoProvider) GetMyPinRooms(userId string) []Room {
	pipeline := bson.A{
		bson.M{"$match": bson.M{"members": bson.M{"$elemMatch": bson.M{"id": userId, "pin": true}}}},
		bson.M{"$sort": bson.M{"created_at": -1}},
		bson.M{"$limit": 5},
	}
	cursor, err := p.db.Collection("rooms").Aggregate(context.TODO(), pipeline)
	if err != nil || cursor == nil {
		return nil
	}
	var docs []bson.M
	cursor.All(context.TODO(), &docs)
	result := make([]Room, 0, len(docs))
	for _, doc := range docs {
		result = append(result, roomFromDoc(doc))
	}
	return result
}

func (p *MongoProvider) GetOlineMyRooms(userId string) []Room {
	filter := bson.M{"members.id": userId}
	opts := options.Find().SetSort(bson.D{{Key: "created_at", Value: -1}})
	cursor, err := p.db.Collection("rooms").Find(context.TODO(), filter, opts)
	if err != nil || cursor == nil {
		log.Printf("GetOlineMyRooms: Find error for user %s: %v", userId, err)
		return nil
	}
	var docs []bson.M
	if err := cursor.All(context.TODO(), &docs); err != nil {
		log.Printf("GetOlineMyRooms: cursor.All error for user %s: %v", userId, err)
		return nil
	}
	result := make([]Room, 0, len(docs))
	for _, doc := range docs {
		result = append(result, roomFromDoc(doc))
	}
	return result
}

func (p *MongoProvider) Insert(room Room) {
	p.InsertRoom(room)
}

func (p *MongoProvider) LastMessage(id string, message Message) bool {
	update := bson.D{{Key: "$set", Value: bson.D{
		{Key: "last_message", Value: bson.M{
			"_id": message.ID, "content": message.Content, "message_type": message.Type,
			"username": message.Username, "client_id": message.ClientID, "created_at": message.CreatedAt,
		}},
		{Key: "updated_at", Value: time.Now()},
	}}}
	_, err := p.db.Collection("rooms").UpdateOne(context.TODO(), bson.M{"id": id}, update)
	return err == nil
}

func (p *MongoProvider) Update(room Room) {
	p.db.Collection("rooms").FindOneAndUpdate(context.TODO(), bson.M{"id": room.ID}, bson.D{{Key: "$set", Value: bson.D{{Key: "status", Value: string(room.Status)}}}})
}

func (p *MongoProvider) UpdateMember(room Room) {
	result := p.db.Collection("rooms").FindOneAndUpdate(context.TODO(), bson.M{"id": room.ID}, bson.D{{Key: "$set", Value: bson.D{{Key: "members", Value: room.Members}}}})
	if err := result.Err(); err != nil {
		log.Printf("UpdateMember: FindOneAndUpdate error for room %s: %v", room.ID, err)
	}
}

func (p *MongoProvider) UpdateRoom(room Room) error {
	filter := bson.M{"id": room.ID}
	updateFields := bson.D{}
	if room.Name != "" {
		updateFields = append(updateFields, bson.E{Key: "name", Value: room.Name})
	}
	if room.Type != "" {
		updateFields = append(updateFields, bson.E{Key: "type", Value: string(room.Type)})
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
	_, err := p.db.Collection("rooms").UpdateOne(context.TODO(), filter, bson.D{{Key: "$set", Value: updateFields}})
	return err
}

func (p *MongoProvider) UpdateRoomAvatar(roomId, avatarUrl string) error {
	_, err := p.db.Collection("rooms").UpdateOne(context.TODO(), bson.M{"id": roomId}, bson.D{{Key: "$set", Value: bson.D{{Key: "avatar_url", Value: avatarUrl}}}})
	return err
}

// --- UserOps ---

func (p *MongoProvider) GetUser(userId string) Member {
	var member Member
	uid, err := strconv.Atoi(userId)
	if err != nil {
		log.Printf("GetUser: invalid userId %s: %v", userId, err)
		return member
	}
	var doc bson.M
	err = p.db.Collection("users").FindOne(context.TODO(), bson.M{"user_id": uid}).Decode(&doc)
	if err != nil {
		log.Printf("GetUser: FindOne error for userId %s: %v", userId, err)
		return member
	}
	member.Id = userId
	if v, ok := doc["firstname"]; ok {
		member.FirstName = toString(v)
	}
	if v, ok := doc["lastname"]; ok {
		member.LastName = toString(v)
	}
	if v, ok := doc["last_seen"]; ok {
		member.LastSeen = toInt64(v)
	}
	return member
}

func (p *MongoProvider) UpdateUser(userId string, user UserDto) {
	uid, _ := strconv.Atoi(userId)
	update := bson.D{{Key: "$set", Value: bson.D{
		{Key: "firstname", Value: user.FirstName},
		{Key: "lastname", Value: user.LastName},
		{Key: "username", Value: user.UserName},
		{Key: "avatar_url", Value: user.AvatarUrl},
	}}}
	p.db.Collection("users").UpdateOne(context.TODO(), bson.M{"user_id": uid}, update)
}

func (p *MongoProvider) EnsureUser(member Member) {
	uid, err := strconv.Atoi(member.Id)
	if err != nil {
		return
	}
	count, _ := p.db.Collection("users").CountDocuments(context.TODO(), bson.M{"user_id": uid})
	if count > 0 {
		return
	}
	doc := bson.D{
		{Key: "user_id", Value: uid},
		{Key: "firstname", Value: member.FirstName},
		{Key: "lastname", Value: member.LastName},
		{Key: "avatar_url", Value: member.AvatarUrl},
	}
	p.db.Collection("users").InsertOne(context.TODO(), doc)
}

func (p *MongoProvider) UpdateLastSeen(userId string, lastSeen int64) {
	uid, _ := strconv.Atoi(userId)
	p.db.Collection("users").UpdateOne(context.TODO(), bson.M{"user_id": uid}, bson.D{{Key: "$set", Value: bson.D{{Key: "last_seen", Value: lastSeen}}}})
}

func (p *MongoProvider) GetUserLastSeen(userId string) int64 {
	uid, err := strconv.Atoi(userId)
	if err != nil {
		return 0
	}
	var doc struct {
		LastSeen int64 `bson:"last_seen"`
	}
	err = p.db.Collection("users").FindOne(context.TODO(), bson.M{"user_id": uid}).Decode(&doc)
	if err != nil {
		return 0
	}
	return doc.LastSeen
}
