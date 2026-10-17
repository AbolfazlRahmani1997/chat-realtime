package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"server/adapters/Reporisotires"
	"server/adapters/Services"
	"server/internal/admin"
	"server/internal/message_broker"
	"server/internal/ws"
	"server/ports/reporisoties"
	"server/router"
	"time"

	"github.com/gocql/gocql"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func initMongo() *mongo.Client {
	mongoUrl := fmt.Sprintf("mongodb://%s:%s/",
		os.Getenv("MONGO_DB_HOST"), os.Getenv("MONGO_DB_PORT"))
	log.Printf("connecting to mongo: %s", mongoUrl)
	credential := options.Credential{
		Username: os.Getenv("MONGO_DB_USERNAME"),
		Password: os.Getenv("MONGO_DB_PASSWORD"),
	}
	clientOptions := options.Client().ApplyURI(mongoUrl).SetAuth(credential)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatalf("mongo connect failed: %v", err)
	}
	if err := client.Ping(context.TODO(), nil); err != nil {
		log.Fatalf("mongo ping failed — cannot reach database: %v", err)
	}
	log.Println("mongo connected successfully")
	return client
}

func getDbName() string {
	dbName := os.Getenv("CHAT_DB")
	if dbName == "" {
		dbName = "chat_db"
	}
	return dbName
}

func initRedis() *redis.Client {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	redisPassword := os.Getenv("REDIS_PASSWORD")
	client := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		Password:     redisPassword,
		DB:           0,
		PoolSize:     64,
		MinIdleConns: 8,
	})
	if _, err := client.Ping(context.TODO()).Result(); err != nil {
		log.Printf("redis connect warning: %v (chat will run without Redis clustering)", err)
		return nil
	}
	log.Println("redis connected successfully")
	return client
}

func main() {
	if err := godotenv.Load(".env"); err != nil {
		log.Print("No .env file found")
	}

	dbDriver := os.Getenv("DB_DRIVER")
	if dbDriver == "" {
		dbDriver = "mongo"
	}

	redisClient := initRedis()

	var roomRepository reporisoties.RoomRepositoryPort
	var messageRepository reporisoties.MessageRepositoryPort
	var mongoClient *mongo.Client

	var dbProvider ws.DatabaseProvider

	switch dbDriver {
	case "scylla", "scylladb":
		var scyllaSession *gocql.Session
		dbProvider, roomRepository, messageRepository, scyllaSession = initScylla()
		_ = scyllaSession
		mongoClient = nil

	default:
		mongoClient = initMongo()
		dbProvider = ws.NewMongoProvider(mongoClient.Database(getDbName()))
		roomRepository = Reporisotires.NewRoomRepository(mongoClient)
		messageRepository = Reporisotires.NewMessageRepository(mongoClient)
	}

	kafkaProducer, _ := messagebroker.SetupProducer()

	var kafkaConsumer *messagebroker.Consumer
	if kafkaProducer != nil {
		if dbProvider != nil {
			ops := dbProvider.MessageOps()
			kc, err := messagebroker.NewConsumer(
				func(id string, content string, msgType string, uniqId string, roomID string, username string, clientID string, deliver []string, read []string, replyToJSON *string, forwardFromJSON *string, createdAt time.Time, updatedAt time.Time) {
					ops.InsertMessageFlat(id, content, msgType, uniqId, roomID, username, clientID, deliver, read, replyToJSON, forwardFromJSON, createdAt, updatedAt)
				},
			)
			if err != nil {
				log.Printf("kafka consumer setup failed: %v (messages will not be persisted)", err)
			} else {
				kafkaConsumer = kc
				kafkaConsumer.Start()
			}
		}
	}

	hub := ws.NewHub(mongoClient, redisClient, dbProvider, kafkaProducer)
	roomService := Services.NewRoomService(roomRepository)
	messageService := Services.NewMessageService(messageRepository)
	adminHandler := admin.NewHandler(hub, roomService, messageService)
	wsHandler := ws.NewHandler(hub)

	go wsHandler.UpdateUserPool()
	go hub.Run()
	go hub.Manager()
	router.InitRouter(wsHandler, adminHandler)
	chatUrl := fmt.Sprintf("0.0.0.0:%s", os.Getenv("CHAT_WS_PORT"))
	fmt.Println(chatUrl)
	err := router.Start(chatUrl)
	if err != nil {
		fmt.Println(err)
		return
	}

	if kafkaConsumer != nil {
		defer kafkaConsumer.Close()
	}
	if kafkaProducer != nil {
		defer kafkaProducer.Close()
	}

}
