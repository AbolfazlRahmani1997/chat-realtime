package mongo

import (
	"context"
	"strconv"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"server/internal/core/domain"
)

type userRepo struct {
	db *mongo.Database
}

func NewUserRepo(db *mongo.Database) *userRepo {
	return &userRepo{db: db}
}

func (r *userRepo) GetByID(userID string) (*domain.User, error) {
	uid, err := strconv.Atoi(userID)
	if err != nil {
		return nil, err
	}

	var doc bson.M
	filter := bson.M{"user_id": uid}
	err = r.db.Collection("users").FindOne(context.TODO(), filter).Decode(&doc)
	if err != nil {
		return nil, err
	}

	user := &domain.User{
		UserId:    userID,
		FirstName: toString(doc["firstname"]),
		LastName:  toString(doc["lastname"]),
		UserName:  toString(doc["username"]),
		AvatarUrl: toString(doc["avatar_url"]),
	}
	if v, ok := doc["last_seen"]; ok {
		user.LastSeen = toInt64(v)
	}
	return user, nil
}

func (r *userRepo) Upsert(user *domain.User) error {
	uid, err := strconv.Atoi(user.UserId)
	if err != nil {
		return err
	}

	update := bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "firstname", Value: user.FirstName},
			{Key: "lastname", Value: user.LastName},
			{Key: "username", Value: user.UserName},
			{Key: "avatar_url", Value: user.AvatarUrl},
		}},
	}
	filter := bson.M{"user_id": uid}
	_, err = r.db.Collection("users").UpdateOne(context.TODO(), filter, update)
	return err
}

func (r *userRepo) UpdateLastSeen(userID string, lastSeen int64) error {
	uid, err := strconv.Atoi(userID)
	if err != nil {
		return err
	}

	update := bson.D{{Key: "$set", Value: bson.D{{Key: "last_seen", Value: lastSeen}}}}
	filter := bson.M{"user_id": uid}
	_, err = r.db.Collection("users").UpdateOne(context.TODO(), filter, update)
	return err
}

func (r *userRepo) GetLastSeen(userID string) (int64, error) {
	uid, err := strconv.Atoi(userID)
	if err != nil {
		return 0, err
	}

	var result struct {
		LastSeen int64 `bson:"last_seen"`
	}
	filter := bson.M{"user_id": uid}
	err = r.db.Collection("users").FindOne(context.TODO(), filter).Decode(&result)
	if err != nil {
		return 0, err
	}
	return result.LastSeen, nil
}
