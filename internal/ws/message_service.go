package ws

import (
	"go.mongodb.org/mongo-driver/mongo"
)

type MessageService struct {
	MessageRepository MessageRepository
	DB                DatabaseProvider
}

// NewMessageService build Service Usage In Hub
func NewMessageService(repository MessageRepository, db DatabaseProvider) MessageService {
	return MessageService{
		MessageRepository: repository,
		DB:                db,
	}
}

// SetMessage Insert Data in Redis And MongoDb (stateless data ,stateFull Data )
func (receiver MessageService) SetMessage(roomId string, messageId string, message *Message) (bool, error) {
	receiver.DB.MessageOps().InsertMessage(*message)
	return receiver.MessageRepository.Redis.SetMessage(roomId, messageId, *message).Val(), nil
}

// GetMessage Get Data From Redis For Paper ChatRoom Message
func (receiver MessageService) GetMessage(roomId string) []interface{} {
	return receiver.MessageRepository.Redis.GetData(roomId).Val()
}

func (receiver MessageService) MessageDelivery(id string, clientIds []string) {
	err := receiver.DB.MessageOps().MessageDelivery(id, clientIds)
	if err != nil {
		return
	}
}

func (receiver MessageService) MessageRead(id string, clientId string) Message {
	message := receiver.DB.MessageOps().GetMessageById(id)
	if message.ID == "" {
		message = receiver.DB.MessageOps().GetMessageByUniqId(id)
	}
	if message.ID == "" {
		return Message{}
	}
	clientIds := message.Read
	if clientIds == nil {
		clientIds = []string{}
	}
	clientIds = append(clientIds, clientId)
	receiver.DB.MessageOps().MessageRead(message.ID, clientIds)
	return message
}

func (r MessageRepository) insertRoomInDb(room Room) *mongo.InsertOneResult {
	return r.Mongo.InsertRoom(room)
}

func (r MessageRepository) getRoom(roomId string) Room {
	return r.GetRoomById(roomId)
}

func (r MessageRepository) updateRoom(roomId string, room Room) {
	r.UpdateRoomById(roomId, room)
}
func (r MessageRepository) getAllMessages(roomId string, userId string) []Message {
	messages := r.Mongo.GetAllMessages(roomId, userId)
	return messages
}
func (r MessageService) getRoomMessages(roomId string, limit string) []Message {
	if r.DB == nil {
		return r.MessageRepository.Mongo.GetRoomMessage(roomId, limit)
	}
	return r.DB.MessageOps().GetRoomMessage(roomId, limit)
}
func (receiver MessageService) getNotDeliverMessage(number int, key string) []Message {
	return receiver.MessageRepository.Redis.GetNotDeliverMessages(number, key)
}
