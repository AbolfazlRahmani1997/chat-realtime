package ws

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/net/context"
)

type RoomMongoRepository struct {
	MongoDbRepository *mongo.Database
}

func NewRoomRepository(client *mongo.Database) RoomMongoRepository {
	return RoomMongoRepository{MongoDbRepository: NewMongoDbRepository(client)}
}

func (r RoomMongoRepository) getById(roomId string) Room {
	var room Room
	filter := bson.M{
		"id": roomId,
	}
	cur := r.MongoDbRepository.Collection("rooms").FindOne(context.TODO(), filter)
	err := cur.Decode(&room)
	if err != nil {
		return Room{}
	}

	return room
}

func (r *RoomMongoRepository) GetMyRooms(userId string, page int, offset int, filterType string) []Room {

	matchStage := bson.M{
		"$match": bson.M{
			"members": bson.M{
				"$elemMatch": bson.M{
					"id":  userId,
					"pin": false,
				},
			},
			"active_status": bson.M{"$ne": "deactive"},
		},
	}

	if filterType != "" {
		matchStage = bson.M{
			"$match": bson.M{
				"members": bson.M{
					"$elemMatch": bson.M{
						"id":  userId,
						"pin": false,
					},
				},
				"type":          filterType,
				"active_status": bson.M{"$ne": "deactive"},
			},
		}
	}

	notPinedRoom := []bson.M{
		matchStage,

		{
			"$sort": bson.M{
				"last_message.created_at": -1,
			},
		},
	}

	var rooms []Room
	//limit, _ := strconv.Atoi(page)
	l := offset
	skip := int64((page * offset) - offset)

	//pinnedRoomsQuery := bson.M{"members": bson.M{"$elemMatch": bson.M{"id": userId, "pin": true}}}
	//matchStage := bson.D{{"$match", pinnedRoomsQuery}}
	//sortStage := bson.D{{"$sort", bson.D{{"members.pin", 1}, {"last_message.created_at", -1}}}}
	skipStage := bson.M{"$skip": skip}
	limitStage := bson.M{"$limit": l}
	notPinedRoom = append(notPinedRoom, skipStage)
	notPinedRoom = append(notPinedRoom, limitStage)
	cur, err := r.MongoDbRepository.Collection("rooms").Aggregate(context.TODO(), notPinedRoom)

	if err != nil {
		fmt.Println(err)
		return rooms
	}

	err = cur.All(context.TODO(), &rooms)
	if err != nil {
		fmt.Println(err)
		return rooms
	}
	return rooms
}
func (r *RoomMongoRepository) GetMyPinRooms(userId string) []Room {
	var rooms []Room
	getPinedMessage := []bson.M{
		{
			"$match": bson.M{
				"members": bson.M{
					"$elemMatch": bson.M{
						"id":  userId,
						"pin": true,
					},
				},
			},
		},
		{
			"$sort": bson.M{
				"last_message.created_at": -1,
			},
		},
	}
	limitStage := bson.M{"$limit": 5}

	getPinedMessage = append(getPinedMessage, limitStage)

	cur, err := r.MongoDbRepository.Collection("rooms").Aggregate(context.TODO(), getPinedMessage)
	fmt.Println(cur)
	if err != nil {
		fmt.Println(err)
		return rooms
	}

	err = cur.All(context.TODO(), &rooms)

	if err != nil {
		fmt.Println(err)
		return rooms
	}
	fmt.Println(rooms)
	return rooms
}
func (r *RoomMongoRepository) GetOlineMyRooms(userId string) []Room {
	var rooms []Room
	filter := bson.M{
		"members.id": userId,
	}
	findOptions := options.FindOptions{}
	opts := findOptions.SetSort(bson.D{{"last_message.created_at", -1}})

	cur, err := r.MongoDbRepository.Collection("rooms").Find(context.TODO(), filter, opts)
	if err != nil {
		fmt.Println(err)
		return rooms
	}
	err = cur.All(context.TODO(), &rooms)
	if err != nil {
		fmt.Println(err)
		return rooms
	}
	return rooms
}

func (r RoomMongoRepository) insert(room Room) interface{} {
	result, err := r.MongoDbRepository.Collection("rooms").InsertOne(context.TODO(), room)
	if err != nil {
		fmt.Println(err)
	}
	return result
}

func (r RoomMongoRepository) lastMessage(id string, message Message) bool {

	_id, _ := primitive.ObjectIDFromHex(id)
	filter := bson.D{{"_id", _id}}
	message.CreatedAt = time.Now()

	update := bson.D{{"$set", bson.D{{"last_message", message}}}}
	_, err := r.MongoDbRepository.Collection("rooms").UpdateOne(context.TODO(), filter, update)
	if err != nil {
		fmt.Println(err)
	}

	return true
}

func (r RoomMongoRepository) update(room Room) *mongo.SingleResult {
	fliter := bson.M{"id": room.ID}
	update := bson.D{{"$set", bson.D{{"status", room.Status}}}}
	result := r.MongoDbRepository.Collection("rooms").FindOneAndUpdate(context.Background(), fliter, update)

	return result
}
func (r RoomMongoRepository) updateMember(room Room) *mongo.SingleResult {

	fliter := bson.M{"id": room.ID}
	update := bson.D{{"$set", bson.D{{"members", room.Members}}}}
	result := r.MongoDbRepository.Collection("rooms").FindOneAndUpdate(context.Background(), fliter, update)
	return result
}

func (r RoomMongoRepository) updateRoom(room Room) error {
	filter := bson.M{"id": room.ID}
	updateFields := bson.D{}
	if room.Name != "" {
		updateFields = append(updateFields, bson.E{Key: "name", Value: room.Name})
	}
	if room.Type != "" {
		updateFields = append(updateFields, bson.E{Key: "type", Value: room.Type})
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
	update := bson.D{{"$set", updateFields}}
	_, err := r.MongoDbRepository.Collection("rooms").UpdateOne(context.TODO(), filter, update)
	return err
}

func (r RoomMongoRepository) updateRoomAvatar(roomId string, avatarUrl string) error {
	filter := bson.M{"id": roomId}
	update := bson.D{{"$set", bson.D{{"avatar_url", avatarUrl}}}}
	_, err := r.MongoDbRepository.Collection("rooms").UpdateOne(context.TODO(), filter, update)
	return err
}
