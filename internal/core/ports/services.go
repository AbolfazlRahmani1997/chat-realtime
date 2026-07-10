package ports

import "server/internal/core/domain"

type ChatService interface {
	SendMessage(roomID string, msg *domain.Message) error
	GetRoomMessages(roomID string, page int) ([]domain.Message, error)
	MarkAsRead(messageID, userID string) (*domain.Message, error)
	DeliverMessage(roomID, userID string) error

	GetUserRooms(userID, page, filterType string) ([]domain.Room, error)
	SyncRoomMembers(room *domain.Room) (*domain.Room, error)
	CreateSupportRoom(userID string) error
	CreateBotRoom(userID string) error
	TogglePin(roomID, userID string) (*domain.Specification, error)
	ToggleNotification(roomID, userID string) (*domain.Specification, error)

	GetUser(userID string) (*domain.User, error)
	UpdateUser(user *domain.User) error
}

type RoomBroker interface {
	PublishRoomCreated(room *domain.Room) error
	ConsumeRoomCreated(handler func(*domain.Room)) error
}
