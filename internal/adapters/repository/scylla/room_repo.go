package scylla

import (
	"time"

	"github.com/gocql/gocql"
	"server/internal/core/domain"
)

type roomRepo struct {
	session *gocql.Session
}

func NewRoomRepo(session *gocql.Session) *roomRepo {
	return &roomRepo{session: session}
}

func (r *roomRepo) saveMembersBatch(roomID string, members []domain.Member, b *gocql.Batch) {
	for _, m := range members {
		b.Query(`INSERT INTO room_members (room_id, user_id, roles, firstname, lastname, avatar_url, notification, pin, last_seen) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			roomID, m.Id, m.Roles, m.FirstName, m.LastName, m.AvatarUrl, m.Notification, m.Pin, m.LastSeen)
	}
}

func (r *roomRepo) roomByRow(id, name, statusStr string, createdAt, updatedAt time.Time) domain.Room {
	return domain.Room{ID: id, Name: name, Status: domain.MemberStatus(statusStr)}
}

func (r *roomRepo) attachRooms(rooms []domain.Room) {
	ids := make([]string, len(rooms))
	for i := range rooms {
		ids[i] = rooms[i].ID
	}

	memberMap := batchLoadMembers(r.session, ids)
	lastMsgMap := batchLoadLastMessages(r.session, ids)

	for i := range rooms {
		rooms[i].Members = memberMap[rooms[i].ID]
		rooms[i].Message = lastMsgMap[rooms[i].ID]
	}
}

func (r *roomRepo) scanAndAttach(iter *gocql.Iter) ([]domain.Room, error) {
	var rooms []domain.Room
	var id, name, statusStr string
	var temp bool
	var createdAt, updatedAt time.Time

	for iter.Scan(&id, &name, &temp, &statusStr, &createdAt, &updatedAt) {
		rooms = append(rooms, r.roomByRow(id, name, statusStr, createdAt, updatedAt))
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}

	r.attachRooms(rooms)
	return rooms, nil
}

func (r *roomRepo) GetByID(id string) (*domain.Room, error) {
	var name, statusStr string
	var temp bool
	var createdAt, updatedAt time.Time

	err := r.session.Query(`SELECT id, name, type, status, created_at, updated_at FROM rooms WHERE id = ?`,
		id).Scan(&id, &name, &temp, &statusStr, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}

	rooms, err := r.scanAndAttach(r.session.Query(`SELECT id, name, type, status, created_at, updated_at FROM rooms WHERE id = ?`, id).Iter())
	if err != nil {
		return nil, err
	}
	if len(rooms) == 0 {
		return nil, gocql.ErrNotFound
	}
	room := rooms[0]
	return &room, nil
}

func (r *roomRepo) GetUserRooms(userID string, page, offset int, filterType string) ([]domain.Room, error) {
	limit := offset
	skip := (page * offset) - offset

	iter := r.session.Query(`SELECT room_id FROM room_members WHERE user_id = ? ALLOW FILTERING`, userID).Iter()
	var ids []string
	var roomID string
	for iter.Scan(&roomID) {
		ids = append(ids, roomID)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}

	memberMap := batchLoadMembers(r.session, ids)
	lastMsgMap := batchLoadLastMessages(r.session, ids)

	roomsIter := r.session.Query(`SELECT id, name, type, status, created_at, updated_at FROM rooms WHERE id IN ?`, ids).Iter()
	var allRooms []domain.Room
	var id, name, statusStr string
	var temp bool
	var createdAt, updatedAt time.Time

	for roomsIter.Scan(&id, &name, &temp, &statusStr, &createdAt, &updatedAt) {
		room := r.roomByRow(id, name, statusStr, createdAt, updatedAt)
		room.Members = memberMap[id]
		room.Message = lastMsgMap[id]

		if filterType != "" && string(room.Type) != filterType {
			continue
		}
		allRooms = append(allRooms, room)
	}
	roomsIter.Close()

	if skip > 0 && len(allRooms) > skip {
		allRooms = allRooms[skip:]
	} else if skip > 0 {
		return nil, nil
	}
	if len(allRooms) > limit {
		allRooms = allRooms[:limit]
	}
	return allRooms, nil
}

func (r *roomRepo) GetPinnedRooms(userID string, limit int) ([]domain.Room, error) {
	iter := r.session.Query(`SELECT room_id FROM room_members WHERE user_id = ? AND pin = true ALLOW FILTERING`, userID).Iter()
	var ids []string
	var roomID string
	for iter.Scan(&roomID) {
		ids = append(ids, roomID)
		if len(ids) >= limit {
			break
		}
	}
	iter.Close()

	if len(ids) == 0 {
		return nil, nil
	}

	return r.scanAndAttach(r.session.Query(`SELECT id, name, type, status, created_at, updated_at FROM rooms WHERE id IN ?`, ids).Iter())
}

func (r *roomRepo) GetOnlineRooms(userID string) ([]domain.Room, error) {
	iter := r.session.Query(`SELECT room_id FROM room_members WHERE user_id = ? ALLOW FILTERING`, userID).Iter()
	var ids []string
	var roomID string
	for iter.Scan(&roomID) {
		ids = append(ids, roomID)
	}
	iter.Close()

	if len(ids) == 0 {
		return nil, nil
	}

	return r.scanAndAttach(r.session.Query(`SELECT id, name, type, status, created_at, updated_at FROM rooms WHERE id IN ?`, ids).Iter())
}

func (r *roomRepo) Save(room *domain.Room) error {
	now := time.Now()
	b := r.session.NewBatch(gocql.LoggedBatch)

	b.Query(`INSERT INTO rooms (id, name, type, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
		room.ID, room.Name, false, string(room.Status), now, now)

	for _, m := range room.Members {
		b.Query(`INSERT INTO room_members (room_id, user_id, roles, firstname, lastname, avatar_url, notification, pin, last_seen) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			room.ID, m.Id, m.Roles, m.FirstName, m.LastName, m.AvatarUrl, m.Notification, m.Pin, m.LastSeen)
	}

	return r.session.ExecuteBatch(b)
}

func (r *roomRepo) UpdateLastMessage(roomID string, msg *domain.Message) error {
	return r.session.Query(`INSERT INTO room_last_message (room_id, message_id, content, message_type, username, client_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		roomID, msg.ID, msg.Content, msg.Type, msg.Username, msg.ClientID, msg.CreatedAt,
	).Exec()
}

func (r *roomRepo) UpdateStatus(roomID string, status domain.MemberStatus) error {
	return r.session.Query(`UPDATE rooms SET status = ?, updated_at = ? WHERE id = ?`,
		string(status), time.Now(), roomID,
	).Exec()
}

func (r *roomRepo) UpdateMembers(roomID string, members []domain.Member) error {
	b := r.session.NewBatch(gocql.LoggedBatch)
	b.Query(`DELETE FROM room_members WHERE room_id = ?`, roomID)
	r.saveMembersBatch(roomID, members, b)
	return r.session.ExecuteBatch(b)
}

func (r *roomRepo) UpdateRoom(room *domain.Room) error {
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
	if err := r.session.Query("UPDATE rooms SET "+setClauses+" WHERE id = ?", args...).Exec(); err != nil {
		return err
	}

	if len(room.Members) > 0 {
		return r.UpdateMembers(room.ID, room.Members)
	}
	return nil
}

func (r *roomRepo) UpdateAvatar(roomID, avatarURL string) error {
	return r.session.Query(`UPDATE rooms SET avatar_url = ?, updated_at = ? WHERE id = ?`,
		avatarURL, time.Now(), roomID,
	).Exec()
}
