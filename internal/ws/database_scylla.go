package ws

import (
	"encoding/json"
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
	var forwardJSON *string
	if message.ForwardFrom != nil {
		if raw, err := json.Marshal(message.ForwardFrom); err == nil {
			s := string(raw)
			forwardJSON = &s
		}
	}
	p.InsertMessageFlat(message.ID, message.Content, message.Type, message.UniqId, message.RoomID, message.Username, message.ClientID, message.Deliver, message.Read, nil, forwardJSON, message.CreatedAt, message.UpdatedAt)
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

	// UnloggedBatch: denormalized fan-out without coordinator round-trips.
	b := p.Session.NewBatch(gocql.UnloggedBatch)
	b.Query(`INSERT INTO messages (id, content, message_type, room_id, username, client_id, uniqid, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, content, msgType, roomID, username, clientID, uniqId, createdAt, updatedAt)
	b.Query(`INSERT INTO messages_by_room (room_id, created_at, id, content, message_type, username, client_id, uniqid) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, createdAt, id, content, msgType, username, clientID, uniqId)
	if uniqId != "" {
		b.Query(`INSERT INTO messages_by_uniqid (uniqid, id) VALUES (?, ?)`, uniqId, id)
	}
	for _, cid := range deliver {
		b.Query(`INSERT INTO message_deliver (message_id, client_id) VALUES (?, ?)`, id, cid)
	}
	for _, cid := range read {
		b.Query(`INSERT INTO message_read (message_id, client_id) VALUES (?, ?)`, id, cid)
	}
	if err := p.Session.ExecuteBatch(b); err != nil {
		log.Printf("Scylla InsertMessageFlat batch error: %v", err)
	}
	if forwardFromJSON != nil && *forwardFromJSON != "" {
		_ = p.Session.Query(
			`INSERT INTO message_meta (message_id, forward_from, edited) VALUES (?, ?, false)`,
			id, *forwardFromJSON,
		).Exec()
	}
}

func (p *ScyllaProvider) loadDeliverAndRead(id string) (deliver, read []string) {
	iter := p.Session.Query(`SELECT client_id FROM message_deliver WHERE message_id = ?`, id).Iter()
	var cid string
	for iter.Scan(&cid) {
		deliver = append(deliver, cid)
	}
	_ = iter.Close()

	iter = p.Session.Query(`SELECT client_id FROM message_read WHERE message_id = ?`, id).Iter()
	for iter.Scan(&cid) {
		read = append(read, cid)
	}
	_ = iter.Close()
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

func (p *ScyllaProvider) scanMessagesByRoom(roomId string, limit int) []Message {
	iter := p.Session.Query(
		`SELECT id, content, message_type, username, client_id, uniqid, created_at FROM messages_by_room WHERE room_id = ? LIMIT ?`,
		roomId, limit,
	).Iter()

	var messages []Message
	var id, content, msgType, username, clientID, uniqid string
	var createdAt time.Time
	for iter.Scan(&id, &content, &msgType, &username, &clientID, &uniqid, &createdAt) {
		d, r := p.loadDeliverAndRead(id)
		messages = append(messages, mapMessageScylla(id, content, msgType, roomId, username, clientID, uniqid, createdAt, createdAt, d, r))
	}
	_ = iter.Close()
	return messages
}

func (p *ScyllaProvider) GetMessageNotDelivery(roomId, userId string) []Message {
	// Partition-key read on messages_by_room — no ALLOW FILTERING.
	recent := p.scanMessagesByRoom(roomId, 100)
	var messages []Message
	for _, m := range recent {
		if m.ClientID == userId {
			continue
		}
		delivered := false
		for _, c := range m.Deliver {
			if c == userId {
				delivered = true
				break
			}
		}
		if !delivered {
			messages = append(messages, m)
		}
	}
	return messages
}

func (p *ScyllaProvider) GetRoomMessages(roomId, userId string) []Message {
	return p.scanMessagesByRoom(roomId, 1)
}

func (p *ScyllaProvider) GetMessageNotCountDelivery(roomId, userId string) int64 {
	return int64(len(p.GetMessageNotDelivery(roomId, userId)))
}

func (p *ScyllaProvider) GetRoomMessage(roomId, page string) []Message {
	limit := 10
	pageInt, _ := strconv.Atoi(page)
	if pageInt < 1 {
		pageInt = 1
	}
	// Fetch only the current page window (avoid over-fetch + skip when possible).
	need := pageInt * limit
	all := p.scanMessagesByRoom(roomId, need)
	skip := (pageInt - 1) * limit
	if skip >= len(all) {
		return nil
	}
	end := skip + limit
	if end > len(all) {
		end = len(all)
	}
	slice := all[skip:end]
	p.attachReactions(slice)
	p.attachMeta(slice)
	return slice
}

func (p *ScyllaProvider) GetAllMessages(roomId, userId string) []Message {
	return p.scanMessagesByRoom(roomId, 10)
}

func (p *ScyllaProvider) upsertMember(roomID string, m Member, roomType Type) {
	_ = p.Session.Query(
		`INSERT INTO room_members (room_id, user_id, roles, firstname, lastname, avatar_url, notification, pin, last_seen) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, m.Id, m.Roles, m.FirstName, m.LastName, m.AvatarUrl, m.Notification, m.Pin, m.LastSeen,
	).Exec()
	_ = p.Session.Query(
		`INSERT INTO rooms_by_user (user_id, room_id, pin, room_type) VALUES (?, ?, ?, ?)`,
		m.Id, roomID, m.Pin, string(roomType),
	).Exec()
}

func (p *ScyllaProvider) InsertRoom(room Room) {
	now := time.Now()
	roomType := string(room.Type)
	if roomType == "" {
		roomType = string(GROUP)
	}
	err := p.Session.Query(
		`INSERT INTO rooms (id, name, type, status, avatar_url, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		room.ID, room.Name, roomType, string(room.Status), room.AvatarUrl, now, now,
	).Exec()
	if err != nil {
		log.Printf("Scylla InsertRoom error: %v", err)
	}
	for _, m := range room.Members {
		p.upsertMember(room.ID, m, room.Type)
	}
}

func (p *ScyllaProvider) GetRoom(roomId string) Room {
	var id, name, roomType, statusStr, avatarURL string
	var createdAt, updatedAt time.Time

	err := p.Session.Query(
		`SELECT id, name, type, status, avatar_url, created_at, updated_at FROM rooms WHERE id = ?`,
		roomId,
	).Scan(&id, &name, &roomType, &statusStr, &avatarURL, &createdAt, &updatedAt)
	if err != nil {
		return Room{}
	}

	room := Room{
		ID: id, Name: name, Type: Type(roomType),
		AvatarUrl: avatarURL, Status: status(statusStr),
	}

	mIter := p.Session.Query(
		`SELECT user_id, roles, firstname, lastname, avatar_url, notification, pin, last_seen FROM room_members WHERE room_id = ?`,
		roomId,
	).Iter()
	var userID, firstname, lastname, memberAvatar string
	var roles []string
	var notification, pin bool
	var lastSeen int64
	for mIter.Scan(&userID, &roles, &firstname, &lastname, &memberAvatar, &notification, &pin, &lastSeen) {
		room.Members = append(room.Members, Member{
			Id: userID, Roles: roles, FirstName: firstname,
			LastName: lastname, AvatarUrl: memberAvatar,
			Notification: notification, Pin: pin, LastSeen: lastSeen,
		})
	}
	_ = mIter.Close()

	var msgID, lcontent, ltype, lusername, lclientID string
	var lcreatedAt time.Time
	if err := p.Session.Query(
		`SELECT message_id, content, message_type, username, client_id, created_at FROM room_last_message WHERE room_id = ?`,
		roomId,
	).Scan(&msgID, &lcontent, &ltype, &lusername, &lclientID, &lcreatedAt); err == nil {
		room.Message = Message{
			ID: msgID, Content: lcontent, Type: ltype,
			Username: lusername, ClientID: lclientID, CreatedAt: lcreatedAt,
		}
	}

	return room
}

func (p *ScyllaProvider) GetMessage(messageId string) Message {
	var id, content, msgType, roomID, username, clientID, uniqid string
	var createdAt, updatedAt time.Time

	err := p.Session.Query(
		`SELECT id, content, message_type, room_id, username, client_id, uniqid, created_at, updated_at FROM messages WHERE id = ?`,
		messageId,
	).Scan(&id, &content, &msgType, &roomID, &username, &clientID, &uniqid, &createdAt, &updatedAt)
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
	var id string
	if err := p.Session.Query(`SELECT id FROM messages_by_uniqid WHERE uniqid = ?`, uniqId).Scan(&id); err != nil {
		return Message{}
	}
	return p.GetMessage(id)
}

func (p *ScyllaProvider) MessageDelivery(id string, clientIds []string) error {
	// Only touch PK-addressable rows. Side-table is source of truth for receipts.
	b := p.Session.NewBatch(gocql.UnloggedBatch)
	b.Query(`UPDATE messages SET delivered = true WHERE id = ?`, id)
	for _, cid := range clientIds {
		b.Query(`INSERT INTO message_deliver (message_id, client_id) VALUES (?, ?)`, id, cid)
	}
	return p.Session.ExecuteBatch(b)
}

func (p *ScyllaProvider) MessageRead(id string, clientIds []string) error {
	b := p.Session.NewBatch(gocql.UnloggedBatch)
	b.Query(`UPDATE messages SET read = true WHERE id = ?`, id)
	for _, cid := range clientIds {
		b.Query(`INSERT INTO message_read (message_id, client_id) VALUES (?, ?)`, id, cid)
	}
	return p.Session.ExecuteBatch(b)
}

// EditMessage rewrites the text of a message the caller already authorized.
func (p *ScyllaProvider) EditMessage(id, content string) (Message, error) {
	msg := p.GetMessage(id)
	if msg.ID == "" {
		return Message{}, gocql.ErrNotFound
	}
	now := time.Now()
	if err := p.Session.Query(
		`UPDATE messages SET content = ?, updated_at = ? WHERE id = ?`,
		content, now, id,
	).Exec(); err != nil {
		return Message{}, err
	}
	if err := p.Session.Query(
		`UPDATE messages_by_room SET content = ? WHERE room_id = ? AND created_at = ? AND id = ?`,
		content, msg.RoomID, msg.CreatedAt, id,
	).Exec(); err != nil {
		return Message{}, err
	}
	if err := p.Session.Query(
		`UPDATE message_meta SET edited = true WHERE message_id = ?`,
		id,
	).Exec(); err != nil {
		return Message{}, err
	}
	var lastID string
	_ = p.Session.Query(`SELECT message_id FROM room_last_message WHERE room_id = ?`, msg.RoomID).Scan(&lastID)
	msg.Content = content
	msg.Edited = true
	msg.UpdatedAt = now
	if lastID == msg.ID || lastID == msg.UniqId {
		p.LastMessage(msg.RoomID, msg)
	}
	return msg, nil
}

func (p *ScyllaProvider) SaveSecretAudience(messageID string, audience []string) error {
	if messageID == "" {
		return gocql.ErrNotFound
	}
	return p.Session.Query(
		`INSERT INTO message_secrets (message_id, audience) VALUES (?, ?)`,
		messageID, audience,
	).Exec()
}

func (p *ScyllaProvider) ViewSecret(messageID, userID string) (content, state string, purgedNow bool, err error) {
	msg := p.GetMessage(messageID)
	if msg.ID == "" || msg.Type != MessageContentTypeSecret {
		return "", "", false, gocql.ErrNotFound
	}
	var audience []string
	if err = p.Session.Query(
		`SELECT audience FROM message_secrets WHERE message_id = ?`,
		msg.ID,
	).Scan(&audience); err != nil {
		return "", "", false, err
	}
	allowed := false
	for _, id := range audience {
		if id == userID {
			allowed = true
			break
		}
	}
	if !allowed {
		return "", "", false, gocql.ErrNotFound
	}
	if msg.Content == "" {
		return "", SecretPurged, false, nil
	}
	var seen string
	err = p.Session.Query(
		`SELECT user_id FROM message_secret_views WHERE message_id = ? AND user_id = ?`,
		msg.ID, userID,
	).Scan(&seen)
	if err == nil {
		return "", SecretViewed, false, nil
	}
	if err != gocql.ErrNotFound {
		return "", "", false, err
	}
	if err = p.Session.Query(
		`INSERT INTO message_secret_views (message_id, user_id) VALUES (?, ?)`,
		msg.ID, userID,
	).Exec(); err != nil {
		return "", "", false, err
	}
	viewed := map[string]bool{userID: true}
	iter := p.Session.Query(
		`SELECT user_id FROM message_secret_views WHERE message_id = ?`,
		msg.ID,
	).Iter()
	var uid string
	for iter.Scan(&uid) {
		viewed[uid] = true
	}
	_ = iter.Close()
	all := len(audience) > 0
	for _, id := range audience {
		if !viewed[id] {
			all = false
			break
		}
	}
	if all {
		_ = p.Session.Query(`UPDATE messages SET content = ? WHERE id = ?`, "", msg.ID).Exec()
		_ = p.Session.Query(
			`UPDATE messages_by_room SET content = ? WHERE room_id = ? AND created_at = ? AND id = ?`,
			"", msg.RoomID, msg.CreatedAt, msg.ID,
		).Exec()
		return msg.Content, SecretLocked, true, nil
	}
	return msg.Content, SecretLocked, false, nil
}

func (p *ScyllaProvider) RedactSecrets(messages []Message, userID string) []Message {
	for i := range messages {
		if messages[i].Type != MessageContentTypeSecret {
			continue
		}
		plain := messages[i].Content
		messages[i].Content = ""
		if plain == "" {
			messages[i].SecretState = SecretPurged
			continue
		}
		var seen string
		err := p.Session.Query(
			`SELECT user_id FROM message_secret_views WHERE message_id = ? AND user_id = ?`,
			messages[i].ID, userID,
		).Scan(&seen)
		if err == nil {
			messages[i].SecretState = SecretViewed
			continue
		}
		messages[i].SecretState = SecretLocked
	}
	return messages
}

func (p *ScyllaProvider) attachMeta(messages []Message) {
	for i := range messages {
		var forwardFrom string
		var edited bool
		err := p.Session.Query(
			`SELECT forward_from, edited FROM message_meta WHERE message_id = ?`,
			messages[i].ID,
		).Scan(&forwardFrom, &edited)
		if err != nil {
			continue
		}
		messages[i].Edited = edited
		if forwardFrom == "" {
			continue
		}
		var info ForwardInfo
		if json.Unmarshal([]byte(forwardFrom), &info) == nil {
			messages[i].ForwardFrom = &info
		}
	}
}

// ToggleReaction sets one emoji per user. Sending the same emoji again removes it.
func (p *ScyllaProvider) ToggleReaction(messageID, roomID, userID, emoji string) ([]Reaction, error) {
	var current string
	err := p.Session.Query(
		`SELECT emoji FROM message_reactions WHERE message_id = ? AND user_id = ?`,
		messageID, userID,
	).Scan(&current)
	if err != nil && err != gocql.ErrNotFound {
		return nil, err
	}
	if err == nil && (emoji == "" || current == emoji) {
		if err := p.Session.Query(
			`DELETE FROM message_reactions WHERE message_id = ? AND user_id = ?`,
			messageID, userID,
		).Exec(); err != nil {
			return nil, err
		}
	} else if emoji != "" {
		if err := p.Session.Query(
			`INSERT INTO message_reactions (message_id, user_id, emoji, room_id) VALUES (?, ?, ?, ?)`,
			messageID, userID, emoji, roomID,
		).Exec(); err != nil {
			return nil, err
		}
	}
	return p.reactionsFor(messageID), nil
}

func (p *ScyllaProvider) attachReactions(messages []Message) {
	for i := range messages {
		messages[i].Reactions = p.reactionsFor(messages[i].ID)
	}
}

func (p *ScyllaProvider) reactionsFor(messageID string) []Reaction {
	iter := p.Session.Query(
		`SELECT user_id, emoji FROM message_reactions WHERE message_id = ?`,
		messageID,
	).Iter()
	grouped := map[string]*Reaction{}
	order := make([]string, 0)
	var userID, emoji string
	for iter.Scan(&userID, &emoji) {
		if emoji == "" {
			continue
		}
		group, ok := grouped[emoji]
		if !ok {
			group = &Reaction{Emoji: emoji, UserIDs: []string{}}
			grouped[emoji] = group
			order = append(order, emoji)
		}
		group.UserIDs = append(group.UserIDs, userID)
		group.Count = len(group.UserIDs)
	}
	_ = iter.Close()
	out := make([]Reaction, 0, len(order))
	for _, emoji := range order {
		out = append(out, *grouped[emoji])
	}
	return out
}

func (p *ScyllaProvider) UpdateRoomById(id string, room Room) {
	_ = p.Session.Query(
		`UPDATE rooms SET status = ?, type = ?, avatar_url = ?, updated_at = ? WHERE id = ?`,
		string(room.Status), string(room.Type), room.AvatarUrl, time.Now(), id,
	).Exec()
	for _, m := range room.Members {
		p.upsertMember(id, m, room.Type)
	}
}

// --- RoomOps ---

func (p *ScyllaProvider) GetById(roomId string) Room {
	return p.GetRoom(roomId)
}

func (p *ScyllaProvider) userRoomIDs(userId string, onlyPin bool, filterType string) []string {
	iter := p.Session.Query(
		`SELECT room_id, pin, room_type FROM rooms_by_user WHERE user_id = ?`,
		userId,
	).Iter()
	var roomID, roomType string
	var pin bool
	var ids []string
	for iter.Scan(&roomID, &pin, &roomType) {
		if onlyPin && !pin {
			continue
		}
		if filterType != "" && roomType != filterType {
			continue
		}
		ids = append(ids, roomID)
	}
	_ = iter.Close()
	return ids
}

func (p *ScyllaProvider) GetMyRooms(userId string, page, offset int, filterType string) []Room {
	limit := offset
	if limit <= 0 {
		limit = 10
	}
	if page < 1 {
		page = 1
	}
	skip := (page - 1) * limit

	ids := p.userRoomIDs(userId, false, filterType)
	if skip >= len(ids) {
		return nil
	}
	end := skip + limit
	if end > len(ids) {
		end = len(ids)
	}

	var rooms []Room
	for _, id := range ids[skip:end] {
		if room := p.GetRoom(id); room.ID != "" {
			rooms = append(rooms, room)
		}
	}
	return rooms
}

func (p *ScyllaProvider) GetMyPinRooms(userId string) []Room {
	ids := p.userRoomIDs(userId, true, "")
	var result []Room
	for _, id := range ids {
		if room := p.GetRoom(id); room.ID != "" {
			result = append(result, room)
			if len(result) >= 5 {
				break
			}
		}
	}
	return result
}

func (p *ScyllaProvider) GetOlineMyRooms(userId string) []Room {
	ids := p.userRoomIDs(userId, false, "")
	var result []Room
	for _, id := range ids {
		if room := p.GetRoom(id); room.ID != "" {
			result = append(result, room)
		}
	}
	return result
}

func (p *ScyllaProvider) Insert(room Room) {
	p.InsertRoom(room)
}

func (p *ScyllaProvider) LastMessage(id string, message Message) bool {
	err := p.Session.Query(
		`INSERT INTO room_last_message (room_id, message_id, content, message_type, username, client_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id, message.ID, message.Content, message.Type, message.Username, message.ClientID, message.CreatedAt,
	).Exec()
	return err == nil
}

func (p *ScyllaProvider) Update(room Room) {
	_ = p.Session.Query(
		`UPDATE rooms SET status = ?, updated_at = ? WHERE id = ?`,
		string(room.Status), time.Now(), room.ID,
	).Exec()
}

func (p *ScyllaProvider) UpdateMember(room Room) {
	// Drop old membership index rows, then rewrite.
	old := p.Session.Query(`SELECT user_id FROM room_members WHERE room_id = ?`, room.ID).Iter()
	var oldUID string
	for old.Scan(&oldUID) {
		_ = p.Session.Query(`DELETE FROM rooms_by_user WHERE user_id = ? AND room_id = ?`, oldUID, room.ID).Exec()
	}
	_ = old.Close()
	_ = p.Session.Query(`DELETE FROM room_members WHERE room_id = ?`, room.ID).Exec()

	for _, m := range room.Members {
		p.upsertMember(room.ID, m, room.Type)
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
	if room.Type != "" {
		setClauses += ", type = ?"
		args = append(args, string(room.Type))
	}

	args = append(args, room.ID)
	if err := p.Session.Query("UPDATE rooms SET "+setClauses+" WHERE id = ?", args...).Exec(); err != nil {
		return err
	}

	if len(room.Members) > 0 {
		p.UpdateMember(room)
	}
	return nil
}

func (p *ScyllaProvider) UpdateRoomAvatar(roomId, avatarUrl string) error {
	return p.Session.Query(
		`UPDATE rooms SET avatar_url = ?, updated_at = ? WHERE id = ?`,
		avatarUrl, time.Now(), roomId,
	).Exec()
}

// --- UserOps ---

func (p *ScyllaProvider) GetUser(userId string) Member {
	var member Member
	if userId == "" {
		return member
	}

	var firstname, lastname, username, avatarUrl string
	var lastSeen int64
	var uid string

	err := p.Session.Query(
		`SELECT user_id, firstname, lastname, username, avatar_url, last_seen FROM users WHERE user_id = ?`,
		userId,
	).Scan(&uid, &firstname, &lastname, &username, &avatarUrl, &lastSeen)
	if err != nil {
		return member
	}

	member.Id = uid
	member.FirstName = firstname
	member.LastName = lastname
	member.AvatarUrl = avatarUrl
	member.LastSeen = lastSeen
	return member
}

func (p *ScyllaProvider) UpdateUser(userId string, user UserDto) {
	if userId == "" {
		return
	}
	_ = p.Session.Query(
		`UPDATE users SET firstname = ?, lastname = ?, username = ?, avatar_url = ? WHERE user_id = ?`,
		user.FirstName, user.LastName, user.UserName, user.AvatarUrl, userId,
	).Exec()
}

func (p *ScyllaProvider) EnsureUser(member Member) {
	if member.Id == "" {
		return
	}
	// Lightweight existence check via PK read (no COUNT aggregate).
	var uid string
	err := p.Session.Query(`SELECT user_id FROM users WHERE user_id = ?`, member.Id).Scan(&uid)
	if err == nil {
		return
	}
	_ = p.Session.Query(
		`INSERT INTO users (user_id, firstname, lastname, avatar_url) VALUES (?, ?, ?, ?)`,
		member.Id, member.FirstName, member.LastName, member.AvatarUrl,
	).Exec()
}

func (p *ScyllaProvider) UpdateLastSeen(userId string, lastSeen int64) {
	if userId == "" {
		return
	}
	_ = p.Session.Query(`UPDATE users SET last_seen = ? WHERE user_id = ?`, lastSeen, userId).Exec()
}

func (p *ScyllaProvider) GetUserLastSeen(userId string) int64 {
	if userId == "" {
		return 0
	}
	var lastSeen int64
	if err := p.Session.Query(`SELECT last_seen FROM users WHERE user_id = ?`, userId).Scan(&lastSeen); err != nil {
		return 0
	}
	return lastSeen
}
