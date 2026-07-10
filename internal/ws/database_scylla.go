package ws

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/gocql/gocql"
)

type ScyllaProvider struct {
	Session *gocql.Session
}

func NewScyllaProvider(session *gocql.Session) *ScyllaProvider {
	return &ScyllaProvider{Session: session}
}

func (p *ScyllaProvider) MessageOps() MessageOps { return p }
func (p *ScyllaProvider) RoomOps() RoomOps       { return p }
func (p *ScyllaProvider) UserOps() UserOps       { return p }

// --- MessageOps ---

func (p *ScyllaProvider) InsertMessage(message Message) {
	p.InsertMessageFlat(message.ID, message.Content, message.Type, message.UniqId, message.RoomID, message.Username, message.ClientID, message.Deliver, message.Read, nil, nil, message.CreatedAt, message.UpdatedAt)
}

func (p *ScyllaProvider) InsertMessageFlat(msgId string, content string, msgType string, uniqId string, roomID string, username string, clientID string, deliver []string, read []string, replyToJSON *string, forwardFromJSON *string, createdAt time.Time, updatedAt time.Time) {
	now := time.Now()
	if createdAt.IsZero() {
		createdAt = now
	}
	if updatedAt.IsZero() {
		updatedAt = now
	}
	id := msgId
	if id == "" {
		id = uniqId
	}
	if id == "" {
		id = newULID()
	}

	err := p.Session.Query(`INSERT INTO messages (id, content, message_type, room_id, username, client_id, uniqid, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, content, msgType, roomID,
		username, clientID, uniqId, createdAt, updatedAt,
	).Exec()
	if err != nil {
		log.Printf("Scylla InsertMessageFlat error: %v", err)
		return
	}

	err = p.Session.Query(`INSERT INTO messages_by_room (room_id, created_at, id, content, message_type, username, client_id, uniqid) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, createdAt, id, content, msgType,
		username, clientID, uniqId,
	).Exec()
	if err != nil {
		log.Printf("Scylla InsertMessageFlat messages_by_room error: %v", err)
	}

	for _, cid := range deliver {
		p.Session.Query(`INSERT INTO message_deliver (message_id, client_id) VALUES (?, ?)`, id, cid).Exec()
	}
	for _, cid := range read {
		p.Session.Query(`INSERT INTO message_read (message_id, client_id) VALUES (?, ?)`, id, cid).Exec()
	}
}

func (p *ScyllaProvider) loadDeliverAndRead(id string) (deliver, read []string) {
	iter := p.Session.Query(`SELECT client_id FROM message_deliver WHERE message_id = ?`, id).Iter()
	var cid string
	for iter.Scan(&cid) {
		deliver = append(deliver, cid)
	}
	iter.Close()

	iter = p.Session.Query(`SELECT client_id FROM message_read WHERE message_id = ?`, id).Iter()
	for iter.Scan(&cid) {
		read = append(read, cid)
	}
	iter.Close()
	return
}

func mapMessageScylla(id, content, msgType, roomID, username, clientID, uniqid string, createdAt, updatedAt time.Time, deliver, read []string) Message {
	return Message{
		ID: id, Content: content, Type: msgType,
		RoomID: roomID, Username: username, ClientID: clientID,
		UniqId: uniqid, Deliver: deliver, Read: read,
		CreatedAt: createdAt, UpdatedAt: updatedAt,
	}
}

func (p *ScyllaProvider) GetMessageNotDelivery(roomId, userId string) []Message {
	iter := p.Session.Query(`SELECT id, content, message_type, room_id, username, client_id, uniqid, created_at, updated_at FROM messages WHERE room_id = ? AND client_id != ? ALLOW FILTERING`,
		roomId, userId).Iter()

	var messages []Message
	var id, content, msgType, roomID, username, clientID, uniqid string
	var createdAt, updatedAt time.Time

	for iter.Scan(&id, &content, &msgType, &roomID, &username, &clientID, &uniqid, &createdAt, &updatedAt) {
		d, _ := p.loadDeliverAndRead(id)
		delivered := false
		for _, c := range d {
			if c == userId {
				delivered = true
				break
			}
		}
		if !delivered {
			messages = append(messages, mapMessageScylla(id, content, msgType, roomID, username, clientID, uniqid, createdAt, updatedAt, d, nil))
		}
	}
	iter.Close()
	return messages
}

func (p *ScyllaProvider) GetRoomMessages(roomId, userId string) []Message {
	iter := p.Session.Query(`SELECT id, content, message_type, room_id, username, client_id, uniqid, created_at, updated_at FROM messages_by_room WHERE room_id = ? LIMIT 1`,
		roomId).Iter()

	var messages []Message
	var id, content, msgType, roomID, username, clientID, uniqid string
	var createdAt, updatedAt time.Time

	for iter.Scan(&id, &content, &msgType, &roomID, &username, &clientID, &uniqid, &createdAt, &updatedAt) {
		d, r := p.loadDeliverAndRead(id)
		messages = append(messages, mapMessageScylla(id, content, msgType, roomID, username, clientID, uniqid, createdAt, updatedAt, d, r))
	}
	iter.Close()
	return messages
}

func (p *ScyllaProvider) GetMessageNotCountDelivery(roomId, userId string) int64 {
	msgs := p.GetMessageNotDelivery(roomId, userId)
	return int64(len(msgs))
}

func (p *ScyllaProvider) GetRoomMessage(roomId, page string) []Message {
	limit := 10
	pageInt, _ := strconv.Atoi(page)
	if pageInt < 1 {
		pageInt = 1
	}
	skip := (pageInt - 1) * limit
	fmt.Println(roomId)
	iter := p.Session.Query(`SELECT id, content, message_type, room_id, username, client_id, uniqid, created_at FROM messages_by_room WHERE room_id = ? ORDER BY created_at DESC LIMIT ?`,
		roomId, limit+skip).Iter()

	var messages []Message
	var id, content, msgType, roomID, username, clientID, uniqid string
	var createdAt, updatedAt time.Time

	for iter.Scan(&id, &content, &msgType, &roomID, &username, &clientID, &uniqid, &createdAt) {
		d, r := p.loadDeliverAndRead(id)
		messages = append(messages, mapMessageScylla(id, content, msgType, roomID, username, clientID, uniqid, createdAt, updatedAt, d, r))
	}
	iter.Close()

	if skip > 0 && len(messages) > skip {
		messages = messages[skip:]
	} else if skip > 0 {
		return nil
	}
	return messages
}

func (p *ScyllaProvider) GetAllMessages(roomId, userId string) []Message {
	iter := p.Session.Query(`SELECT id, content, message_type, room_id, username, client_id, uniqid, created_at, updated_at FROM messages_by_room WHERE room_id = ? LIMIT 10`,
		roomId).Iter()

	var messages []Message
	var id, content, msgType, roomID, username, clientID, uniqid string
	var createdAt, updatedAt time.Time

	for iter.Scan(&id, &content, &msgType, &roomID, &username, &clientID, &uniqid, &createdAt, &updatedAt) {
		d, r := p.loadDeliverAndRead(id)
		messages = append(messages, mapMessageScylla(id, content, msgType, roomID, username, clientID, uniqid, createdAt, updatedAt, d, r))
	}
	iter.Close()
	return messages
}

func (p *ScyllaProvider) InsertRoom(room Room) {
	now := time.Now()
	err := p.Session.Query(`INSERT INTO rooms (id, name, type, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
		room.ID, room.Name, false, string(room.Status), now, now,
	).Exec()
	if err != nil {
		log.Printf("Scylla InsertRoom error: %v", err)
	}

	for _, m := range room.Members {
		p.Session.Query(`INSERT INTO room_members (room_id, user_id, roles, firstname, lastname, avatar_url, notification, pin, last_seen) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			room.ID, m.Id, m.Roles, m.FirstName, m.LastName, m.AvatarUrl, m.Notification, m.Pin, m.LastSeen,
		).Exec()
	}
}

func (p *ScyllaProvider) GetRoom(roomId string) Room {
	var id, name, statusStr string
	var temp bool
	var createdAt, updatedAt time.Time

	err := p.Session.Query(`SELECT id, name, type, status, created_at, updated_at FROM rooms WHERE id = ?`,
		roomId).Scan(&id, &name, &temp, &statusStr, &createdAt, &updatedAt)
	if err != nil {
		return Room{}
	}

	room := Room{ID: id, Name: name, Status: status(statusStr)}

	mIter := p.Session.Query(`SELECT user_id, roles, firstname, lastname, avatar_url, notification, pin, last_seen FROM room_members WHERE room_id = ?`, roomId).Iter()
	var userID, firstname, lastname, avatarURL string
	var roles []string
	var notification, pin bool
	var lastSeen int64
	for mIter.Scan(&userID, &roles, &firstname, &lastname, &avatarURL, &notification, &pin, &lastSeen) {
		room.Members = append(room.Members, Member{
			Id: userID, Roles: roles, FirstName: firstname,
			LastName: lastname, AvatarUrl: avatarURL,
			Notification: notification, Pin: pin, LastSeen: lastSeen,
		})
	}
	mIter.Close()

	lIter := p.Session.Query(`SELECT message_id, content, message_type, username, client_id, created_at FROM room_last_message WHERE room_id = ?`, roomId).Iter()
	var msgID, lcontent, ltype, lusername, lclientID string
	var lcreatedAt time.Time
	if lIter.Scan(&msgID, &lcontent, &ltype, &lusername, &lclientID, &lcreatedAt) {
		room.Message = Message{
			ID: msgID, Content: lcontent, Type: ltype,
			Username: lusername, ClientID: lclientID, CreatedAt: lcreatedAt,
		}
	}
	lIter.Close()

	return room
}

func (p *ScyllaProvider) GetMessage(messageId string) Message {
	var id, content, msgType, roomID, username, clientID, uniqid string
	var createdAt, updatedAt time.Time

	err := p.Session.Query(`SELECT id, content, message_type, room_id, username, client_id, uniqid, created_at, updated_at FROM messages WHERE id = ?`,
		messageId).Scan(&id, &content, &msgType, &roomID, &username, &clientID, &uniqid, &createdAt, &updatedAt)
	if err != nil {
		return Message{}
	}

	d, r := p.loadDeliverAndRead(id)
	return mapMessageScylla(id, content, msgType, roomID, username, clientID, uniqid, createdAt, updatedAt, d, r)
}

func (p *ScyllaProvider) GetMessageById(id string) Message {
	return p.GetMessage(id)
}

func (p *ScyllaProvider) GetMessageByUniqId(uniqId string) Message {
	var id, content, msgType, roomID, username, clientID, uniqid string
	var createdAt, updatedAt time.Time

	err := p.Session.Query(`SELECT id, content, message_type, room_id, username, client_id, uniqid, created_at, updated_at FROM messages WHERE uniqid = ? ALLOW FILTERING`,
		uniqId).Scan(&id, &content, &msgType, &roomID, &username, &clientID, &uniqid, &createdAt, &updatedAt)
	if err != nil {
		return Message{}
	}

	d, r := p.loadDeliverAndRead(id)
	return mapMessageScylla(id, content, msgType, roomID, username, clientID, uniqid, createdAt, updatedAt, d, r)
}

func (p *ScyllaProvider) MessageDelivery(id string, clientIds []string) error {
	b := p.Session.NewBatch(gocql.LoggedBatch)
	b.Query(`UPDATE messages SET delivered = true WHERE id = ?`, id)
	b.Query(`UPDATE messages_by_room SET delivered = true WHERE id = ?`, id)
	for _, cid := range clientIds {
		b.Query(`INSERT INTO message_deliver (message_id, client_id) VALUES (?, ?)`, id, cid)
	}
	return p.Session.ExecuteBatch(b)
}

func (p *ScyllaProvider) MessageRead(id string, clientIds []string) error {
	b := p.Session.NewBatch(gocql.LoggedBatch)
	b.Query(`UPDATE messages SET read = true WHERE id = ?`, id)
	b.Query(`UPDATE messages_by_room SET read = true WHERE id = ?`, id)
	for _, cid := range clientIds {
		b.Query(`INSERT INTO message_read (message_id, client_id) VALUES (?, ?)`, id, cid)
	}
	return p.Session.ExecuteBatch(b)
}

func (p *ScyllaProvider) UpdateRoomById(id string, room Room) {
	p.Session.Query(`UPDATE rooms SET status = ?, updated_at = ? WHERE id = ?`, string(room.Status), time.Now(), id).Exec()

	for _, m := range room.Members {
		p.Session.Query(`INSERT INTO room_members (room_id, user_id, roles, firstname, lastname, avatar_url, notification, pin, last_seen) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			id, m.Id, m.Roles, m.FirstName, m.LastName, m.AvatarUrl, m.Notification, m.Pin, m.LastSeen,
		).Exec()
	}
}

// --- RoomOps ---

func (p *ScyllaProvider) GetById(roomId string) Room {
	return p.GetRoom(roomId)
}

func (p *ScyllaProvider) GetMyRooms(userId string, page, offset int, filterType string) []Room {
	limit := offset
	skip := (page * offset) - offset

	mIter := p.Session.Query(`SELECT room_id FROM room_members WHERE user_id = ? ALLOW FILTERING`, userId).Iter()
	var roomID string
	var allRooms []Room

	for mIter.Scan(&roomID) {
		room := p.GetRoom(roomID)
		if room.ID == "" {
			continue
		}
		if filterType != "" && string(room.Type) != filterType {
			continue
		}
		allRooms = append(allRooms, room)
	}
	mIter.Close()

	if skip > 0 && len(allRooms) > skip {
		allRooms = allRooms[skip:]
	} else if skip > 0 {
		return nil
	}
	if len(allRooms) > limit {
		allRooms = allRooms[:limit]
	}
	return allRooms
}

func (p *ScyllaProvider) GetMyPinRooms(userId string) []Room {
	iter := p.Session.Query(`SELECT room_id FROM room_members WHERE user_id = ? AND pin = true ALLOW FILTERING`, userId).Iter()
	var roomID string
	var result []Room

	for iter.Scan(&roomID) {
		room := p.GetRoom(roomID)
		if room.ID != "" {
			result = append(result, room)
			if len(result) >= 5 {
				break
			}
		}
	}
	iter.Close()
	return result
}

func (p *ScyllaProvider) GetOlineMyRooms(userId string) []Room {
	iter := p.Session.Query(`SELECT room_id FROM room_members WHERE user_id = ? ALLOW FILTERING`, userId).Iter()
	var roomID string
	var result []Room

	for iter.Scan(&roomID) {
		room := p.GetRoom(roomID)
		if room.ID != "" {
			result = append(result, room)
		}
	}
	iter.Close()
	return result
}

func (p *ScyllaProvider) Insert(room Room) {
	p.InsertRoom(room)
}

func (p *ScyllaProvider) LastMessage(id string, message Message) bool {
	err := p.Session.Query(`INSERT INTO room_last_message (room_id, message_id, content, message_type, username, client_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id, message.ID, message.Content, message.Type, message.Username, message.ClientID, message.CreatedAt,
	).Exec()
	return err == nil
}

func (p *ScyllaProvider) Update(room Room) {
	p.Session.Query(`UPDATE rooms SET status = ?, updated_at = ? WHERE id = ?`, string(room.Status), time.Now(), room.ID).Exec()
}

func (p *ScyllaProvider) UpdateMember(room Room) {
	p.Session.Query(`DELETE FROM room_members WHERE room_id = ?`, room.ID).Exec()
	for _, m := range room.Members {
		p.Session.Query(`INSERT INTO room_members (room_id, user_id, roles, firstname, lastname, avatar_url, notification, pin, last_seen) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			room.ID, m.Id, m.Roles, m.FirstName, m.LastName, m.AvatarUrl, m.Notification, m.Pin, m.LastSeen,
		).Exec()
	}
}

func (p *ScyllaProvider) UpdateRoom(room Room) error {
	args := []interface{}{time.Now()}
	setClauses := "updated_at = ?"

	if room.Name != "" {
		setClauses += ", name = ?"
		args = append(args, room.Name)
	}
	if room.AvatarUrl != "" {
		setClauses += ", avatar_url = ?"
		args = append(args, room.AvatarUrl)
	}

	args = append(args, room.ID)
	err := p.Session.Query("UPDATE rooms SET "+setClauses+" WHERE id = ?", args...).Exec()
	if err != nil {
		return err
	}

	if len(room.Members) > 0 {
		p.UpdateMember(room)
	}
	return nil
}

func (p *ScyllaProvider) UpdateRoomAvatar(roomId, avatarUrl string) error {
	return p.Session.Query(`UPDATE rooms SET avatar_url = ?, updated_at = ? WHERE id = ?`, avatarUrl, time.Now(), roomId).Exec()
}

// --- UserOps ---

func (p *ScyllaProvider) GetUser(userId string) Member {
	var member Member
	uid, err := strconv.Atoi(userId)
	if err != nil {
		return member
	}

	var firstname, lastname, username, avatarUrl string
	var lastSeen int64

	err = p.Session.Query(`SELECT user_id, firstname, lastname, username, avatar_url, last_seen FROM users WHERE user_id = ?`,
		uid).Scan(&uid, &firstname, &lastname, &username, &avatarUrl, &lastSeen)
	if err != nil {
		return member
	}

	member.Id = userId
	member.FirstName = firstname
	member.LastName = lastname
	member.LastSeen = lastSeen
	return member
}

func (p *ScyllaProvider) UpdateUser(userId string, user UserDto) {
	uid, _ := strconv.Atoi(userId)
	p.Session.Query(`UPDATE users SET firstname = ?, lastname = ?, username = ?, avatar_url = ? WHERE user_id = ?`,
		user.FirstName, user.LastName, user.UserName, user.AvatarUrl, uid).Exec()
}

func (p *ScyllaProvider) EnsureUser(member Member) {
	uid, err := strconv.Atoi(member.Id)
	if err != nil {
		return
	}

	var count int
	p.Session.Query(`SELECT COUNT(*) FROM users WHERE user_id = ?`, uid).Scan(&count)
	if count > 0 {
		return
	}

	p.Session.Query(`INSERT INTO users (user_id, firstname, lastname, avatar_url) VALUES (?, ?, ?, ?)`,
		uid, member.FirstName, member.LastName, member.AvatarUrl).Exec()
}

func (p *ScyllaProvider) UpdateLastSeen(userId string, lastSeen int64) {
	uid, _ := strconv.Atoi(userId)
	p.Session.Query(`UPDATE users SET last_seen = ? WHERE user_id = ?`, lastSeen, uid).Exec()
}

func (p *ScyllaProvider) GetUserLastSeen(userId string) int64 {
	uid, err := strconv.Atoi(userId)
	if err != nil {
		return 0
	}

	var lastSeen int64
	err = p.Session.Query(`SELECT last_seen FROM users WHERE user_id = ?`, uid).Scan(&lastSeen)
	if err != nil {
		return 0
	}
	return lastSeen
}
