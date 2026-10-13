package ScyllaDB

import (
	"log"
	"time"

	"github.com/gocql/gocql"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"server/Dtos/DtoReporitories"
	"server/entity/Message"
)

type MessageRepository struct {
	Session *gocql.Session
}

func NewMessageRepository(session *gocql.Session) *MessageRepository {
	return &MessageRepository{Session: session}
}

func (r *MessageRepository) loadReceipts(id string) (deliver, read []string) {
	iter := r.Session.Query(`SELECT client_id FROM message_deliver WHERE message_id = ?`, id).Iter()
	var cid string
	for iter.Scan(&cid) {
		deliver = append(deliver, cid)
	}
	_ = iter.Close()

	iter = r.Session.Query(`SELECT client_id FROM message_read WHERE message_id = ?`, id).Iter()
	for iter.Scan(&cid) {
		read = append(read, cid)
	}
	_ = iter.Close()
	return
}

func mapEntityMessage(id, content, msgType, roomID, username, clientID string, createdAt, updatedAt time.Time, deliver, read []string) Message.Message {
	msg := Message.Message{
		Content: content, Type: msgType, RoomID: roomID,
		Username: username, ClientID: clientID,
		Deliver: deliver, Read: read,
		CreatedAt: createdAt, UpdatedAt: updatedAt,
	}
	if oid, err := primitive.ObjectIDFromHex(id); err == nil {
		msg.ID = oid
	}
	return msg
}

func (r *MessageRepository) GetMessage(id string) Message.Message {
	if id == "" {
		return Message.Message{}
	}

	var msgID, content, msgType, roomID, username, clientID, uniqid string
	var createdAt, updatedAt time.Time

	err := r.Session.Query(
		`SELECT id, content, message_type, room_id, username, client_id, uniqid, created_at, updated_at FROM messages WHERE id = ?`,
		id,
	).Scan(&msgID, &content, &msgType, &roomID, &username, &clientID, &uniqid, &createdAt, &updatedAt)
	if err != nil {
		// Fallback: treat input as uniqid (same as Mongo path).
		var resolved string
		if err2 := r.Session.Query(`SELECT id FROM messages_by_uniqid WHERE uniqid = ?`, id).Scan(&resolved); err2 != nil {
			if err != gocql.ErrNotFound {
				log.Printf("ScyllaDB GetMessage error: %v", err)
			}
			return Message.Message{}
		}
		return r.GetMessage(resolved)
	}

	d, rd := r.loadReceipts(msgID)
	_ = uniqid
	return mapEntityMessage(msgID, content, msgType, roomID, username, clientID, createdAt, updatedAt, d, rd)
}

func (r *MessageRepository) GetRoomMessages(page int, offset int, filter DtoReporitories.MessageFilterDto) []Message.Message {
	if page < 1 {
		page = 1
	}
	if offset <= 0 {
		offset = 10
	}
	if filter.RoomId == "" {
		return []Message.Message{}
	}

	limit := offset
	skip := (page - 1) * offset

	iter := r.Session.Query(
		`SELECT id, content, message_type, username, client_id, uniqid, created_at FROM messages_by_room WHERE room_id = ? LIMIT ?`,
		filter.RoomId, limit+skip,
	).Iter()

	var messages []Message.Message
	var id, content, msgType, username, clientID, uniqid string
	var createdAt time.Time

	for iter.Scan(&id, &content, &msgType, &username, &clientID, &uniqid, &createdAt) {
		d, rd := r.loadReceipts(id)
		msg := mapEntityMessage(id, content, msgType, filter.RoomId, username, clientID, createdAt, createdAt, d, rd)
		if filter.ClientId != "" && msg.ClientID != filter.ClientId {
			continue
		}
		messages = append(messages, msg)
		_ = uniqid
	}
	if err := iter.Close(); err != nil {
		log.Printf("ScyllaDB GetRoomMessages iter error: %v", err)
		return []Message.Message{}
	}

	if skip >= len(messages) {
		return []Message.Message{}
	}
	end := skip + limit
	if end > len(messages) {
		end = len(messages)
	}
	return messages[skip:end]
}
