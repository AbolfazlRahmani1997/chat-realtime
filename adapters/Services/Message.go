package Services

import (
	"server/Dtos/DtoReporitories"
	"server/entity/Message"
	"server/ports/reporisoties"
)

type MessageService struct {
	MessageRepository reporisoties.MessageRepositoryPort
}

func NewMessageService(port reporisoties.MessageRepositoryPort) *MessageService {
	return &MessageService{MessageRepository: port}
}

func (service MessageService) GetMessage(id string) Message.Message {
	var message Message.Message
	message = service.MessageRepository.GetMessage(id)
	return message
}

func (service MessageService) GetAllMessages(page int, dto DtoReporitories.MessageFilterDto) []Message.Message {
	var Messages []Message.Message
	Messages = service.MessageRepository.GetRoomMessages(page, 10, dto)
	return Messages
}
