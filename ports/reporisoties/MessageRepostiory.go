package reporisoties

import (
	"server/Dtos/DtoReporitories"
	"server/entity/Message"
)

type MessageRepositoryPort interface {
	GetMessage(id string) Message.Message
	GetRoomMessages(page int, offset int, filter DtoReporitories.MessageFilterDto) []Message.Message
}
