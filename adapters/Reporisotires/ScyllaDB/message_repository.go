package ScyllaDB

import (
	"encoding/json"
	"log"
	"time"

	"github.com/gocql/gocql"
	"server/Dtos/DtoReporitories"
	"server/entity/Message"
)

type MessageRepository struct {
	Session *gocql.Session
}

func NewMessageRepository(session *gocql.Session) *MessageRepository {
	return &MessageRepository{Session: session}
}

func (r *MessageRepository) GetMessage(id string) Message.Message {
	if id == "" {
		return Message.Message{}
	}

	var msg Message.Message
	var deliverJSON, readJSON string
	var createdAt, updatedAt time.Time

	err := r.Session.Query(`
		SELECT id, content, message_type, room_id, username, client_id, deliver, read, created_at, updated_at
		FROM messages WHERE id = ?
	`, id).Scan(
		&msg.ID, &msg.Content, &msg.Type, &msg.RoomID,
		&msg.Username, &msg.ClientID, &deliverJSON, &readJSON,
		&createdAt, &updatedAt,
	)
	if err != nil {
		if err != gocql.ErrNotFound {
			log.Printf("ScyllaDB GetMessage error: %v", err)
		}
		return Message.Message{}
	}

	msg.CreatedAt = createdAt
	msg.UpdatedAt = updatedAt

	if deliverJSON != "" {
		json.Unmarshal([]byte(deliverJSON), &msg.Deliver)
	}
	if readJSON != "" {
		json.Unmarshal([]byte(readJSON), &msg.Read)
	}

	return msg
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

	iter := r.Session.Query(`
		SELECT id, content, message_type, room_id, username, client_id, deliver, read, created_at, updated_at
		FROM messages_by_room
		WHERE room_id = ?
		ORDER BY created_at DESC, id ASC
		LIMIT ?
	`, filter.RoomId, limit+skip).Iter()

	var messages []Message.Message
	var createdAt, updatedAt time.Time
	var msg Message.Message
	var deliverJSON, readJSON string

	for iter.Scan(
		&msg.ID, &msg.Content, &msg.Type, &msg.RoomID,
		&msg.Username, &msg.ClientID, &deliverJSON, &readJSON,
		&createdAt, &updatedAt,
	) {
		msg.CreatedAt = createdAt
		msg.UpdatedAt = updatedAt
		if deliverJSON != "" {
			json.Unmarshal([]byte(deliverJSON), &msg.Deliver)
		}
		if readJSON != "" {
			json.Unmarshal([]byte(readJSON), &msg.Read)
		}
		messages = append(messages, msg)
		deliverJSON = ""
		readJSON = ""
	}

	if err := iter.Close(); err != nil {
		log.Printf("ScyllaDB GetRoomMessages iter error: %v", err)
		return []Message.Message{}
	}

	if skip > 0 && len(messages) > skip {
		messages = messages[skip:]
	} else if skip > 0 && len(messages) <= skip {
		return []Message.Message{}
	}

	if filter.ClientId != "" {
		var filtered []Message.Message
		for _, m := range messages {
			if m.ClientID == filter.ClientId {
				filtered = append(filtered, m)
			}
		}
		return filtered
	}

	return messages
}
