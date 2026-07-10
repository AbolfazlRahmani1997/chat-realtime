package scylla

import (
	"github.com/gocql/gocql"
	"server/internal/core/domain"
)

func batchLoadDeliver(session *gocql.Session, ids []string) map[string][]string {
	if len(ids) == 0 {
		return nil
	}
	result := make(map[string][]string, len(ids))
	for _, id := range ids {
		result[id] = nil
	}

	iter := session.Query(`SELECT message_id, client_id FROM message_deliver WHERE message_id IN ?`, ids).Iter()
	var msgID, clientID string
	for iter.Scan(&msgID, &clientID) {
		result[msgID] = append(result[msgID], clientID)
	}
	iter.Close()
	return result
}

func batchLoadRead(session *gocql.Session, ids []string) map[string][]string {
	if len(ids) == 0 {
		return nil
	}
	result := make(map[string][]string, len(ids))
	for _, id := range ids {
		result[id] = nil
	}

	iter := session.Query(`SELECT message_id, client_id FROM message_read WHERE message_id IN ?`, ids).Iter()
	var msgID, clientID string
	for iter.Scan(&msgID, &clientID) {
		result[msgID] = append(result[msgID], clientID)
	}
	iter.Close()
	return result
}

type memberRow struct {
	UserID string
	Roles  []string
	FName  string
	LName  string
	Avatar string
	Notif  bool
	Pin    bool
	LS     int64
}

func batchLoadMembers(session *gocql.Session, roomIDs []string) map[string][]domain.Member {
	if len(roomIDs) == 0 {
		return nil
	}
	result := make(map[string][]domain.Member, len(roomIDs))
	for _, id := range roomIDs {
		result[id] = nil
	}

	iter := session.Query(`SELECT room_id, user_id, roles, firstname, lastname, avatar_url, notification, pin, last_seen FROM room_members WHERE room_id IN ?`, roomIDs).Iter()
	var roomID, userID, firstname, lastname, avatarURL string
	var roles []string
	var notification, pin bool
	var lastSeen int64

	for iter.Scan(&roomID, &userID, &roles, &firstname, &lastname, &avatarURL, &notification, &pin, &lastSeen) {
		result[roomID] = append(result[roomID], domain.Member{
			Id: userID, Roles: roles, FirstName: firstname,
			LastName: lastname, AvatarUrl: avatarURL,
			Notification: notification, Pin: pin, LastSeen: lastSeen,
		})
	}
	iter.Close()
	return result
}

type lastMsgRow struct {
	RoomID    string
	MsgID     string
	Content   string
	Type      string
	Username  string
	ClientID  string
	CreatedAt int64
}

func batchLoadLastMessages(session *gocql.Session, roomIDs []string) map[string]domain.Message {
	if len(roomIDs) == 0 {
		return nil
	}
	result := make(map[string]domain.Message, len(roomIDs))

	iter := session.Query(`SELECT room_id, message_id, content, message_type, username, client_id, created_at FROM room_last_message WHERE room_id IN ?`, roomIDs).Iter()
	var roomID, msgID, content, msgType, username, clientID string
	var createdAt int64

	for iter.Scan(&roomID, &msgID, &content, &msgType, &username, &clientID, &createdAt) {
		result[roomID] = domain.Message{
			ID: msgID, Content: content, Type: msgType,
			Username: username, ClientID: clientID,
		}
	}
	iter.Close()
	return result
}
