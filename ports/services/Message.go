package services

import (
	"server/Dtos/DtoReporitories"
	"server/entity/Message"
)

// In Clena Artichect Is Port = interface
type MessageServicePort interface {
	GetMessage(id string) Message.Message
	GetAllMessages(page int, dto DtoReporitories.MessageFilterDto) []Message.Message
}
