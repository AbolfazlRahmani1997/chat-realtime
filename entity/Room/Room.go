package Room

import (
	"os"
	"server/entity/Message"
	"server/enums"
	"server/internal/ws"
	"server/transform/api"
	"server/transform/api/room"
)

type Room struct {
	ID             string           `json:"Id" `
	Name           string           `json:"name" `
	Temporary      bool             `json:"type" `
	Members        []ws.Member      `json:"members"`
	Message        Message.Message  `json:"message" bson:"last_message"`
	Status         enums.Status     `json:"status,omitempty" `
	ActiveStatus   enums.RoomStatus `json:"active_status,omitempty" bson:"active_status"`
	collectionName string
}

func (Room *Room) init() {

	Room.collectionName = os.Getenv("ROOM_COLLECTION")
}

func (Room *Room) ToTransformer() api.Transformer {
	return &room.Room{
		ID:        Room.ID,
		Name:      Room.Name,
		Temporary: Room.Temporary,
		Members:   Room.Members,
		Status:    Room.Status,
		Message:   &Room.Message,
	}
}

func (Room *Room) GetTableName() string {
	return Room.collectionName
}
