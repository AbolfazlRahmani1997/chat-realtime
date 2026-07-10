package ports

import "server/internal/core/domain"

type MessageRepository interface {
	InsertMessage(msg *domain.Message) error
	GetMessageByID(id string) (*domain.Message, error)
	GetMessageByUniqId(uniqId string) (*domain.Message, error)
	GetRoomMessages(roomID string, page, limit int) ([]domain.Message, error)
	GetUndeliveredMessages(roomID, userID string) ([]domain.Message, error)
	GetUndeliveredCount(roomID, userID string) (int64, error)
	UpdateDelivery(id string, clientIDs []string) error
	UpdateReadReceipt(id string, clientIDs []string) error
	GetDeliverDetails(id string) ([]string, error)
	GetReadDetails(id string) ([]string, error)
}

type RoomRepository interface {
	GetByID(id string) (*domain.Room, error)
	GetUserRooms(userID string, page, offset int, filterType string) ([]domain.Room, error)
	GetPinnedRooms(userID string, limit int) ([]domain.Room, error)
	GetOnlineRooms(userID string) ([]domain.Room, error)
	Save(room *domain.Room) error
	UpdateLastMessage(roomID string, msg *domain.Message) error
	UpdateStatus(roomID string, status domain.MemberStatus) error
	UpdateMembers(roomID string, members []domain.Member) error
	UpdateRoom(room *domain.Room) error
	UpdateAvatar(roomID, avatarURL string) error
}

type UserRepository interface {
	GetByID(userID string) (*domain.User, error)
	Upsert(user *domain.User) error
	UpdateLastSeen(userID string, lastSeen int64) error
	GetLastSeen(userID string) (int64, error)
}
