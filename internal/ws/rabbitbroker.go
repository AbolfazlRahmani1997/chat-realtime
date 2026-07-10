package ws

import (
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"sync"
)

type RabbitMqBroker struct {
	Room            chan *Room
	connection      amqp.Connection
	MongoRepository MessageRepository
	DB              DatabaseProvider
}

func NewRabbitMqBroker(Room chan *Room, repository MessageRepository, db DatabaseProvider) RabbitMqBroker {

	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		user := os.Getenv("RABBITMQ_USER")
		pass := os.Getenv("RABBITMQ_PASSWORD")
		host := os.Getenv("RABBITMQ_HOST")
		port := os.Getenv("RABBITMQ_PORT")
		vhost := os.Getenv("RABBITMQ_VHOST")

		if port == "" {
			port = "5672"
		}
		if vhost == "" {
			vhost = ""
		}

		rabbitURL = fmt.Sprintf("amqp://%s:%s@%s:%s/%s", user, pass, host, port, vhost)
	}

	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("failed to connect to RabbitMQ: %v", err)
	}
	return RabbitMqBroker{connection: *conn,
		MongoRepository: repository,
		Room:            Room,
		DB:              db}
}
func (receiver *RabbitMqBroker) Consume() {
	ch, _ := receiver.connection.Channel()

	mes, err := ch.Consume(os.Getenv("RABBITMQ_QUEUE"), "", true, false, false, false, nil)

	if err != nil {
		fmt.Println(err)
	}

	var wg sync.WaitGroup
	go func() {
		defer wg.Done()
		wg.Add(1)
		for d := range mes {
			var RoomRequest RoomBrokerDto
			err = json.Unmarshal(d.Body, &RoomRequest)
			if err != nil {
				fmt.Println(err)
			}
			room := Room{
				ID:      RoomRequest.Id,
				Name:    RoomRequest.Name,
				Members: RoomRequest.Member,
				Type:    RoomRequest.Type,
			}

			roomExist := receiver.DB.MessageOps().GetRoom(room.ID)

			if roomExist.ID == "" {
				receiver.DB.MessageOps().InsertRoom(roomExist)

				receiver.Room <- &room
			}

		}
		wg.Wait()

	}()

}
