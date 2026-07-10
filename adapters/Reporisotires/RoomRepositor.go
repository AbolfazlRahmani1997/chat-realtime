package Reporisotires

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/net/context"
	"os"
	"server/Dtos"
	"server/entity/Room"
)

type RoomMongoRepository struct {
	Collection *mongo.Database
}

func NewRoomRepository(Collection *mongo.Client) RoomMongoRepository {
	dbName := os.Getenv("CHAT_DB")
	if dbName == "" {
		dbName = "chat_db"
	}
	return RoomMongoRepository{Collection: Collection.Database(dbName)}
}

func (receiver RoomMongoRepository) GetRoom(id string) Room.Room {
	var room Room.Room
	condition := bson.M{"id": id}
	find := receiver.Collection.Collection("rooms").FindOne(context.TODO(), condition)
	err := find.Decode(&room)
	if err != nil {
		fmt.Println(err)
		return Room.Room{}
	}
	return room
}

func (receiver RoomMongoRepository) GetAllRooms(page int, offset int, filter Dtos.GetAllRoomFilterDto) []Room.Room {
	var room []Room.Room
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
		SetSort(bson.D{{Key: "last_message.created_at", Value: -1}})

	find, err := receiver.Collection.Collection("rooms").Find(context.TODO(), filter.GetFilter(), opts)
	if err != nil {
		fmt.Println(err)
		return []Room.Room{}
	}

	if err := find.All(context.TODO(), &room); err != nil {
		fmt.Println(err)
		return []Room.Room{}
	}

	return room
}

func (receiver RoomMongoRepository) Update(Update Dtos.UpdateRoomDto) []Room.Room {

	result := receiver.Collection.Collection("rooms").FindOneAndUpdate(context.TODO(), bson.M{"id": Update.Id}, bson.D{{"$set", Update.GetUpdate()}})
	fmt.Println(result.Err())
	return nil
}
