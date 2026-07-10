package scylla

import (
	"encoding/json"
	"time"

	"github.com/gocql/gocql"
	"server/internal/core/domain"
)

type messageRepo struct {
	session *gocql.Session
}

func NewMessageRepo(session *gocql.Session) *messageRepo {
	return &messageRepo{session: session}
}

func replyToJSON(replyTo *domain.ReplyInfo) string {
	if replyTo == nil {
		return ""
	}
	data, _ := json.Marshal(replyTo)
	return string(data)
}

func parseReplyTo(s string) *domain.ReplyInfo {
	if s == "" {
		return nil
	}
	var r domain.ReplyInfo
	if err := json.Unmarshal([]byte(s), &r); err != nil {
		return nil
	}
	return &r
}

func forwardFromJSON(ff *domain.ForwardInfo) string {
	if ff == nil {
		return ""
	}
	data, _ := json.Marshal(ff)
	return string(data)
}

func parseForwardFrom(s string) *domain.ForwardInfo {
	if s == "" {
		return nil
	}
	var f domain.ForwardInfo
	if err := json.Unmarshal([]byte(s), &f); err != nil {
		return nil
	}
	return &f
}

func (r *messageRepo) InsertMessage(msg *domain.Message) error {
	now := time.Now()
	msgID := msg.UniqId
	delivered := len(msg.Deliver) > 0
	read := len(msg.Read) > 0

	b := r.session.NewBatch(gocql.LoggedBatch)

	b.Query(`INSERT INTO messages (id, content, message_type, room_id, username, client_id, uniqid, delivered, read, reply_to, forward_from, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msgID, msg.Content, msg.Type, msg.RoomID, msg.Username, msg.ClientID, msg.UniqId, delivered, read, replyToJSON(msg.ReplyTo), forwardFromJSON(msg.ForwardFrom), now, now)

	b.Query(`INSERT INTO messages_by_room (room_id, created_at, id, content, message_type, username, client_id, uniqid, delivered, read, reply_to, forward_from) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msg.RoomID, now, msgID, msg.Content, msg.Type, msg.Username, msg.ClientID, msg.UniqId, delivered, read, replyToJSON(msg.ReplyTo), forwardFromJSON(msg.ForwardFrom))

	return r.session.ExecuteBatch(b)
}

func scanMessageRow(id, content, msgType, roomID, username, clientID, uniqid string, delivered, read bool, replyTo, forwardFrom string, createdAt, updatedAt time.Time) domain.Message {
	return domain.Message{
		ID: id, Content: content, Type: msgType, RoomID: roomID,
		Username: username, ClientID: clientID, UniqId: uniqid,
		ReplyTo: parseReplyTo(replyTo), ForwardFrom: parseForwardFrom(forwardFrom),
		CreatedAt: createdAt, UpdatedAt: updatedAt,
	}
}

func (r *messageRepo) GetMessageByID(id string) (*domain.Message, error) {
	var content, msgType, roomID, username, clientID, uniqid string
	var delivered, read bool
	var replyTo, forwardFrom string
	var createdAt, updatedAt time.Time

	err := r.session.Query(`SELECT id, content, message_type, room_id, username, client_id, uniqid, delivered, read, reply_to, forward_from, created_at, updated_at FROM messages WHERE id = ?`,
		id).Scan(&id, &content, &msgType, &roomID, &username, &clientID, &uniqid, &delivered, &read, &replyTo, &forwardFrom, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}

	msg := scanMessageRow(id, content, msgType, roomID, username, clientID, uniqid, delivered, read, replyTo, forwardFrom, createdAt, updatedAt)
	return &msg, nil
}

func (r *messageRepo) GetMessageByUniqId(uniqId string) (*domain.Message, error) {
	var id, content, msgType, roomID, username, clientID, uniqid string
	var delivered, read bool
	var replyTo, forwardFrom string
	var createdAt, updatedAt time.Time

	err := r.session.Query(`SELECT id, content, message_type, room_id, username, client_id, uniqid, delivered, read, reply_to, forward_from, created_at, updated_at FROM messages WHERE uniqid = ? ALLOW FILTERING`,
		uniqId).Scan(&id, &content, &msgType, &roomID, &username, &clientID, &uniqid, &delivered, &read, &replyTo, &forwardFrom, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}

	msg := scanMessageRow(id, content, msgType, roomID, username, clientID, uniqid, delivered, read, replyTo, forwardFrom, createdAt, updatedAt)
	return &msg, nil
}

func (r *messageRepo) GetRoomMessages(roomID string, page, limit int) ([]domain.Message, error) {
	skip := (page - 1) * limit
	iter := r.session.Query(`SELECT id, content, message_type, room_id, username, client_id, uniqid, delivered, read, reply_to, forward_from, created_at, updated_at FROM messages_by_room WHERE room_id = ? ORDER BY created_at DESC LIMIT ?`,
		roomID, limit+skip).Iter()

	var messages []domain.Message
	var id, content, msgType, username, clientID, uniqid string
	var delivered, read bool
	var replyTo, forwardFrom string
	var createdAt, updatedAt time.Time

	for iter.Scan(&id, &content, &msgType, &roomID, &username, &clientID, &uniqid, &delivered, &read, &replyTo, &forwardFrom, &createdAt, &updatedAt) {
		messages = append(messages, scanMessageRow(id, content, msgType, roomID, username, clientID, uniqid, delivered, read, replyTo, forwardFrom, createdAt, updatedAt))
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}

	if skip > 0 && len(messages) > skip {
		messages = messages[skip:]
	} else if skip > 0 {
		return nil, nil
	}
	return messages, nil
}

func (r *messageRepo) GetUndeliveredMessages(roomID, userID string) ([]domain.Message, error) {
	iter := r.session.Query(`SELECT id, content, message_type, room_id, username, client_id, uniqid, delivered, read, reply_to, forward_from, created_at, updated_at FROM messages WHERE room_id = ? AND client_id != ? AND delivered = false ALLOW FILTERING`,
		roomID, userID).Iter()

	var messages []domain.Message
	var id, content, msgType, username, clientID, uniqid string
	var delivered, read bool
	var replyTo, forwardFrom string
	var createdAt, updatedAt time.Time

	for iter.Scan(&id, &content, &msgType, &roomID, &username, &clientID, &uniqid, &delivered, &read, &replyTo, &forwardFrom, &createdAt, &updatedAt) {
		messages = append(messages, scanMessageRow(id, content, msgType, roomID, username, clientID, uniqid, delivered, read, replyTo, forwardFrom, createdAt, updatedAt))
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return messages, nil
}

func (r *messageRepo) GetUndeliveredCount(roomID, userID string) (int64, error) {
	var count int64
	err := r.session.Query(`SELECT COUNT(*) FROM messages WHERE room_id = ? AND client_id != ? AND delivered = false ALLOW FILTERING`,
		roomID, userID).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (r *messageRepo) UpdateDelivery(id string, clientIDs []string) error {
	b := r.session.NewBatch(gocql.LoggedBatch)
	b.Query(`UPDATE messages SET delivered = true WHERE id = ?`, id)
	b.Query(`UPDATE messages_by_room SET delivered = true WHERE id = ?`, id)
	return r.session.ExecuteBatch(b)
}

func (r *messageRepo) UpdateReadReceipt(id string, clientIDs []string) error {
	b := r.session.NewBatch(gocql.LoggedBatch)
	b.Query(`UPDATE messages SET read = true WHERE id = ?`, id)
	b.Query(`UPDATE messages_by_room SET read = true WHERE id = ?`, id)
	return r.session.ExecuteBatch(b)
}

func (r *messageRepo) GetDeliverDetails(id string) ([]string, error) {
	iter := r.session.Query(`SELECT client_id FROM message_deliver WHERE message_id = ?`, id).Iter()
	var result []string
	var cid string
	for iter.Scan(&cid) {
		result = append(result, cid)
	}
	return result, iter.Close()
}

func (r *messageRepo) GetReadDetails(id string) ([]string, error) {
	iter := r.session.Query(`SELECT client_id FROM message_read WHERE message_id = ?`, id).Iter()
	var result []string
	var cid string
	for iter.Scan(&cid) {
		result = append(result, cid)
	}
	return result, iter.Close()
}
