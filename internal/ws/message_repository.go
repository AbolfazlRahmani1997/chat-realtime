package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MessageRepository struct {
	Mongo MongoDBRepository
	Redis RedisRepository
}

type MongoDBRepository struct {
	Collection *mongo.Database
}

func NewMongoDbRepository(client *mongo.Database) *mongo.Database {
	return client

}

func NewMessageRepository(client *mongo.Database) MessageRepository {

	return MessageRepository{
		Mongo: MongoDBRepository{NewMongoDbRepository(client)},
		Redis: NewRedisRepository(),
	}

}

// InsertInDb Insert In Db For StateFull
func (r MessageRepository) insertMessageInDb(message Message) *mongo.InsertOneResult {
	message.CreatedAt = time.Now()
	return r.Mongo.insertMessage(message)
}

func (r MongoDBRepository) insertMessage(message Message) *mongo.InsertOneResult {
	one, err := r.Collection.Collection("messages").InsertOne(context.TODO(), message)
	if err != nil {
		return nil
	}
	return one
}

func (r MongoDBRepository) GetMessageNotDelivery(roomId string, userId string) []Message {
	var data []Message
	condition := bson.M{"roomID": roomId, "Deliver": bson.M{"$eq": nil}, "clientID": bson.M{"$ne": userId}}
	opts := options.Find()
	cur, err := r.Collection.Collection("messages").Find(context.Background(), condition, opts)
	if err != nil {

		return nil
	}
	err = cur.All(context.TODO(), &data)
	if err != nil {

		return data
	}
	return data
}
func (r MongoDBRepository) GetRoomMessages(roomId string, userId string) []Message {
	var data []Message
	condition := bson.M{"roomID": roomId}
	opts := options.Find().SetLimit(1)
	cur, err := r.Collection.Collection("messages").Find(context.Background(), condition, opts)
	if err != nil {

		return nil
	}
	err = cur.All(context.TODO(), &data)
	if err != nil {

		return data
	}
	return data
}
func (r MongoDBRepository) GetMessageNotCountDelivery(roomId string, userId string) int64 {
	condition := bson.M{"roomID": roomId, "Deliver": bson.M{"$eq": nil}, "clientID": bson.M{"$ne": userId}}
	opts := options.Count().SetHint("_id_")
	cur, err := r.Collection.Collection("messages").CountDocuments(context.TODO(), condition, opts)
	if err != nil {

		return 0
	}
	return cur
}
func (r MongoDBRepository) GetRoomMessage(roomId string, page string) []Message {
	var data []Message
	limit, _ := strconv.Atoi(page)
	condition := bson.M{"roomID": roomId}
	l := int64(10)
	skip := int64(limit*10 - 10)
	findOptions := options.FindOptions{Skip: &skip, Limit: &l}
	opts := findOptions.SetSort(bson.D{{"created_at", -1}})

	cur, err := r.Collection.Collection("messages").Find(context.Background(), condition, opts)
	if err != nil {
		return nil
	}
	err = cur.All(context.TODO(), &data)
	if err != nil {
		return data
	}
	return data
}

func (r MongoDBRepository) GetAllMessages(roomId string, userId string) []Message {
	var data []Message
	condition := bson.M{"roomID": roomId}

	opts := options.Find().SetLimit(10)
	cur, err := r.Collection.Collection("messages").Find(context.Background(), condition, opts)
	if err != nil {
		return nil
	}
	err = cur.All(context.TODO(), &data)
	if err != nil {
		return data
	}
	return data
}

func (r MongoDBRepository) InsertRoom(room Room) *mongo.InsertOneResult {
	one, err := r.Collection.Collection("rooms").InsertOne(context.TODO(), room)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	return one
}

func (r MongoDBRepository) getRoom(roomId string) Room {

	var roomResult RoomModel
	filter := bson.M{"id": roomId}

	one := r.Collection.Collection("rooms").FindOne(context.Background(), filter)
	data, _ := one.Raw()
	err := json.Unmarshal([]byte(data.String()), &roomResult)
	if err != nil {
		return Room{}
	}
	return Room{
		_Id:     roomResult.ID,
		ID:      roomId,
		Name:    roomResult.Name,
		Type:    roomResult.Type,
		Status:  roomResult.Status,
		Members: roomResult.Members}
}
func (r MongoDBRepository) getMessage(messageId string) Message {
	var message Message
	filter := bson.M{"_id": messageId}
	one := r.Collection.Collection("messages").FindOne(context.Background(), filter)
	err := one.Decode(&message)
	if err != nil {

		return Message{}
	}
	return message
}

type RedisRepository struct {
	Redis *redis.Client
	ctx   context.Context
}

func NewRedisRepository() RedisRepository {
	return RedisRepository{Redis: redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	}),
		ctx: context.Background(),
	}
}

func (r RedisRepository) SetData(key string, value interface{}) {
	r.Redis.Set(r.ctx, key, value, 0)
}
func (r RedisRepository) GetData(key string) *redis.SliceCmd {
	data := r.Redis.HRandField(r.ctx, key, 100)
	return r.Redis.HMGet(r.ctx, key, data.Val()...)
}

func (r RedisRepository) SetMessage(roomId string, messageId string, message Message) *redis.BoolCmd {
	out, err := json.Marshal(message)
	if err != nil {
		fmt.Println(err)
	}
	return r.Redis.HMSet(r.ctx, roomId, messageId, string(out))
}

func (r MessageRepository) GetRoomById(roomId string) Room {
	return r.Mongo.getRoom(roomId)
}
func (r MessageRepository) MessageDelivery(id string, clientIds []string) (*mongo.UpdateResult, error) {
	_id, _ := primitive.ObjectIDFromHex(id)
	filter := bson.D{{"_id", _id}}
	update := bson.D{{"$set", bson.D{{"Deliver", clientIds}}}}
	result, err := r.Mongo.Collection.Collection("messages").UpdateOne(context.TODO(), filter, update)
	if err != nil {
		return nil, err
	}
	return result, nil
}
func (r MessageRepository) MessageRead(id string, clientIds []string) (*mongo.UpdateResult, error) {
	_id, _ := primitive.ObjectIDFromHex(id)

	filter := bson.D{{"_id", _id}}
	update := bson.D{{"$set", bson.D{{"Read", clientIds}}}}
	result, err := r.Mongo.Collection.Collection("messages").UpdateOne(context.TODO(), filter, update)
	if err != nil {
		return nil, err
	}
	return result, err
}

func (r RedisRepository) GetLen(key string) int64 {
	return r.Redis.LLen(context.TODO(), key).Val()
}

func (r RedisRepository) GetMessage(key string) string {
	return r.Redis.LPop(r.ctx, key).Val()
}
func (r RedisRepository) GetNotDeliverMessages(number int, key string) []Message {
	var messages []Message
	var message Message
	for i := 0; i < number; i++ {
		itemMessage := r.Redis.LPop(context.TODO(), key).Val()
		if itemMessage != "" {
			err := json.Unmarshal([]byte(itemMessage), &message)
			if err != nil {
				return nil
			}
		}

		messages = append(messages, message)

	}

	return messages
}
func (r MessageRepository) getNumberNotDelivered(key string) int64 {
	return r.Redis.GetLen(key)
}

func (r MessageRepository) UpdateRoomById(id string, room Room) *mongo.UpdateResult {
	filter := bson.M{"id": id}
	update := bson.D{{"$set", bson.D{{"status", room.Status}, {"members", room.Members}, {"last_message", room.Message}}}}
	result, err := r.Mongo.Collection.Collection("rooms").UpdateOne(context.TODO(), filter, update)
	if err != nil {
		return nil
	}
	return result
}

func (r MessageRepository) getMessageById(id string) Message {
	var message Message
	_id, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return Message{}
	}
	filter := bson.D{{"_id", _id}}
	one := r.Mongo.Collection.Collection("messages").FindOne(context.Background(), filter)
	if one.Err() != nil {
		return Message{}
	}
	if err := one.Decode(&message); err != nil {
		return Message{}
	}
	return message
}

// getMessageByUniqId finds a message by its UniqId (ulid) for seenMessage when frontend sends ulid instead of _id.
func (r MessageRepository) getMessageByUniqId(uniqId string) Message {
	if uniqId == "" {
		return Message{}
	}
	var message Message
	filter := bson.M{"uniqid": uniqId}
	one := r.Mongo.Collection.Collection("messages").FindOne(context.Background(), filter)
	if one.Err() != nil {
		return Message{}
	}
	if err := one.Decode(&message); err != nil {
		return Message{}
	}
	return message
}
