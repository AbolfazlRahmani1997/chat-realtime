package ws

import (
	"fmt"
	"strconv"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/net/context"
)

type UserRepository struct {
	database *mongo.Database
}

type UserDto struct {
	UserId    string `json:"Id,omitempty"`
	UserName  string `json:"Username"`
	FirstName string `json:"Firstname"`
	LastName  string `json:"LastName"`
	AvatarUrl string `json:"AvatarUrl"`
	LastSeen  int64  `json:"LastSeen,omitempty" bson:"last_seen"`
}

func NewUserRepository(client *mongo.Database) UserRepository {
	return UserRepository{database: client}
}

func (receiver UserRepository) getUser(userid string) Member {
	var model Member
	userId, err := strconv.Atoi(userid)
	filter := bson.M{"user_id": userId}
	one := receiver.database.Collection("users").FindOne(context.TODO(), filter)
	err = one.Decode(&model)
	if err != nil {
		return model
	}

	return model

}

func (receiver UserRepository) UpdateUser(userid string, user UserDto) {

	userId, _ := strconv.Atoi(userid)
	update := bson.D{{"$set", bson.D{{"firstname", user.FirstName}, {"lastname", user.LastName}, {"username", user.UserName}, {"avatar_url", user.AvatarUrl}}}}
	filter := bson.M{"user_id": userId}
	_, err := receiver.database.Collection("users").UpdateOne(context.TODO(), filter, update)
	if err != nil {
	}
}

func (receiver UserRepository) ensureUser(member Member) {
	userId, err := strconv.Atoi(member.Id)
	if err != nil {
		return
	}
	fmt.Println(userId)
	filter := bson.M{"user_id": userId}
	count, err := receiver.database.Collection("users").CountDocuments(context.TODO(), filter)
	if err != nil || count > 0 {
		return
	}
	doc := bson.D{
		{"user_id", userId},
		{"firstname", member.FirstName},
		{"lastname", member.LastName},
		{"avatar_url", member.AvatarUrl},
	}
	_, err = receiver.database.Collection("users").InsertOne(context.TODO(), doc)
	if err != nil {
	}
}

func (receiver UserRepository) UpdateLastSeen(userid string, lastSeen int64) {
	userId, _ := strconv.Atoi(userid)
	filter := bson.M{"user_id": userId}
	update := bson.D{{"$set", bson.D{{"last_seen", lastSeen}}}}
	_, err := receiver.database.Collection("users").UpdateOne(context.TODO(), filter, update)
	if err != nil {
	}
}

func (receiver UserRepository) getUserLastSeen(userid string) int64 {
	var result struct {
		LastSeen int64 `bson:"last_seen"`
	}
	userId, err := strconv.Atoi(userid)
	if err != nil {
		return 0
	}
	filter := bson.M{"user_id": userId}
	err = receiver.database.Collection("users").FindOne(context.TODO(), filter).Decode(&result)
	if err != nil {
		return 0
	}
	return result.LastSeen
}
