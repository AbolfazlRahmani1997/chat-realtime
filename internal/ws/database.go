package ws

import "time"

type DatabaseProvider interface {
	MessageOps() MessageOps
	RoomOps() RoomOps
	UserOps() UserOps
}

type MessageOps interface {
	InsertMessage(message Message)
	InsertMessageFlat(msgId string, content string, msgType string, uniqId string, roomID string, username string, clientID string, deliver []string, read []string, replyToJSON *string, forwardFromJSON *string, createdAt time.Time, updatedAt time.Time)
	GetMessageNotDelivery(roomId, userId string) []Message
	GetRoomMessages(roomId, userId string) []Message
	GetMessageNotCountDelivery(roomId, userId string) int64
	GetRoomMessage(roomId, page string) []Message
	GetAllMessages(roomId, userId string) []Message
	InsertRoom(room Room)
	GetRoom(roomId string) Room
	GetMessage(messageId string) Message
	GetMessageById(id string) Message
	GetMessageByUniqId(uniqId string) Message
	MessageDelivery(id string, clientIds []string) error
	MessageRead(id string, clientIds []string) error
	UpdateRoomById(id string, room Room)
}

type RoomOps interface {
	GetById(roomId string) Room
	GetMyRooms(userId string, page, offset int, filterType string) []Room
	GetMyPinRooms(userId string) []Room
	GetOlineMyRooms(userId string) []Room
	Insert(room Room)
	LastMessage(id string, message Message) bool
	Update(room Room)
	UpdateMember(room Room)
	UpdateRoom(room Room) error
	UpdateRoomAvatar(roomId, avatarUrl string) error
}

type UserOps interface {
	GetUser(userId string) Member
	UpdateUser(userId string, user UserDto)
	EnsureUser(member Member)
	UpdateLastSeen(userId string, lastSeen int64)
	GetUserLastSeen(userId string) int64
}
