package mongo

import (
	"log"
	"os"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

func NewDatabase(client *mongo.Client) *mongo.Database {
	dbName := os.Getenv("CHAT_DB")
	if dbName == "" {
		dbName = "chat_db"
	}
	return client.Database(dbName)
}

func ObjectIDFromHex(s string) primitive.ObjectID {
	oid, err := primitive.ObjectIDFromHex(s)
	if err == nil {
		return oid
	}
	return primitive.NewObjectID()
}

func init() {
	log.Println("mongo adapter loaded")
}
