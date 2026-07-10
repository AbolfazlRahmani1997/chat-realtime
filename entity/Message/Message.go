package Message

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
	"os"
	"server/transform/api"
	"server/transform/api/message"
	"time"
)

type ReplyInfo struct {
	ID       string `json:"id" bson:"id"`
	Content  string `json:"content" bson:"content"`
	Username string `json:"username" bson:"username"`
}

type ForwardInfo struct {
	FromRoomID string `json:"from_room_id" bson:"from_room_id"`
	FromMsgID  string `json:"from_msg_id" bson:"from_msg_id"`
	Content    string `json:"content" bson:"content"`
	Username   string `json:"username" bson:"username"`
}

type Message struct {
	ID             primitive.ObjectID `json:"_id" bson:"_id,omitempty"`
	Content        string             `json:"Content,omitempty"  bson:"content"`
	Type           string             `json:"Type,omitempty" bson:"message_type,omitempty"`
	RoomID         string             `json:"RoomID,omitempty"  bson:"roomID"`
	Username       string             `json:"Username,omitempty" bson:"username" `
	ClientID       string             `json:"ClientID,omitempty" bson:"clientID"`
	Deliver        []string           `json:"Deliver,omitempty" bson:"Deliver"`
	Read           []string           `json:"Read,omitempty" bson:"Read"`
	ReplyTo        *ReplyInfo         `json:"ReplyTo,omitempty" bson:"reply_to,omitempty"`
	ForwardFrom    *ForwardInfo       `json:"ForwardFrom,omitempty" bson:"forward_from,omitempty"`
	connectionId   string
	CreatedAt      time.Time `json:"CreatedAt" bson:"created_at"`
	UpdatedAt      time.Time `bson:"updated_at"`
	collectionName string
}

func (Message *Message) init() {
	Message.collectionName = os.Getenv("MESSAGE_COLLECTION")
}
func (Message *Message) ToTransformer() api.Transformer {
	return &message.Message{}

}

func (Message *Message) GetTableName() string {
	return Message.collectionName
}

func (Message *Message) ToCollectionTransformer() api.Transformer {
	return &message.Message{}

}
