package room

import (
	"server/entity"
	"server/enums"
	"server/internal/ws"
)
import _ "server/transform/api/member"

type Room struct {
	ID        string        `json:"Id" `
	Name      string        `json:"name" `
	Temporary bool          `json:"type" `
	Members   []ws.Member   `json:"members"`
	Message   entity.Entity `json:"message" bson:"last_message"`
	Status    enums.Status  `json:"status,omitempty" `
}

func (receiver *Room) Get() {

}
