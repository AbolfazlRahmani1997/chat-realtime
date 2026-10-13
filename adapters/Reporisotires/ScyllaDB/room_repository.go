package ScyllaDB

import (
	"log"
	"time"

	"github.com/gocql/gocql"
	"server/Dtos"
	"server/entity/Message"
	"server/entity/Room"
	"server/enums"
	"server/internal/ws"
)

type RoomRepository struct {
	Session *gocql.Session
}

func NewRoomRepository(session *gocql.Session) *RoomRepository {
	return &RoomRepository{Session: session}
}

func (r *RoomRepository) loadMembers(roomID string) []ws.Member {
	iter := r.Session.Query(
		`SELECT user_id, roles, firstname, lastname, avatar_url, notification, pin, last_seen FROM room_members WHERE room_id = ?`,
		roomID,
	).Iter()
	var members []ws.Member
	var userID, firstname, lastname, avatarURL string
	var roles []string
	var notification, pin bool
	var lastSeen int64
	for iter.Scan(&userID, &roles, &firstname, &lastname, &avatarURL, &notification, &pin, &lastSeen) {
		members = append(members, ws.Member{
			Id: userID, Roles: roles, FirstName: firstname,
			LastName: lastname, AvatarUrl: avatarURL,
			Notification: notification, Pin: pin, LastSeen: lastSeen,
		})
	}
	_ = iter.Close()
	return members
}

func (r *RoomRepository) loadLastMessage(roomID string) Message.Message {
	var msgID, content, msgType, username, clientID string
	var createdAt time.Time
	err := r.Session.Query(
		`SELECT message_id, content, message_type, username, client_id, created_at FROM room_last_message WHERE room_id = ?`,
		roomID,
	).Scan(&msgID, &content, &msgType, &username, &clientID, &createdAt)
	if err != nil {
		return Message.Message{}
	}
	return Message.Message{
		Content: content, Type: msgType, RoomID: roomID,
		Username: username, ClientID: clientID, CreatedAt: createdAt,
	}
}

func (r *RoomRepository) GetRoom(id string) Room.Room {
	if id == "" {
		return Room.Room{}
	}

	var room Room.Room
	var roomType, statusStr string
	var createdAt, updatedAt time.Time

	err := r.Session.Query(
		`SELECT id, name, type, status, created_at, updated_at FROM rooms WHERE id = ?`,
		id,
	).Scan(&room.ID, &room.Name, &roomType, &statusStr, &createdAt, &updatedAt)
	if err != nil {
		if err != gocql.ErrNotFound {
			log.Printf("ScyllaDB GetRoom error: %v", err)
		}
		return Room.Room{}
	}

	room.Temporary = roomType == string(ws.TEMP)
	room.Status = enums.Status(statusStr)
	room.Members = r.loadMembers(room.ID)
	room.Message = r.loadLastMessage(room.ID)
	_ = createdAt
	_ = updatedAt
	return room
}

func (r *RoomRepository) GetAllRooms(page int, offset int, filter Dtos.GetAllRoomFilterDto) []Room.Room {
	if page < 1 {
		page = 1
	}
	if offset <= 0 {
		offset = 10
	}
	skip := (page - 1) * offset

	// Membership filter uses partition-key lookup on rooms_by_user.
	if filter.MemberId != "" {
		iter := r.Session.Query(
			`SELECT room_id FROM rooms_by_user WHERE user_id = ?`,
			filter.MemberId,
		).Iter()
		var roomID string
		var rooms []Room.Room
		for iter.Scan(&roomID) {
			if filter.Id != "" && roomID != filter.Id {
				continue
			}
			room := r.GetRoom(roomID)
			if room.ID != "" {
				rooms = append(rooms, room)
			}
		}
		_ = iter.Close()
		if skip >= len(rooms) {
			return []Room.Room{}
		}
		end := skip + offset
		if end > len(rooms) {
			end = len(rooms)
		}
		return rooms[skip:end]
	}

	if filter.Id != "" {
		room := r.GetRoom(filter.Id)
		if room.ID == "" {
			return []Room.Room{}
		}
		return []Room.Room{room}
	}

	// Full scan only when no filter — admin use-case, bounded by LIMIT.
	iter := r.Session.Query(`SELECT id FROM rooms LIMIT ?`, skip+offset).Iter()
	var id string
	var rooms []Room.Room
	for iter.Scan(&id) {
		room := r.GetRoom(id)
		if room.ID != "" {
			rooms = append(rooms, room)
		}
	}
	_ = iter.Close()

	if skip >= len(rooms) {
		return []Room.Room{}
	}
	end := skip + offset
	if end > len(rooms) {
		end = len(rooms)
	}
	return rooms[skip:end]
}

func (r *RoomRepository) Update(update Dtos.UpdateRoomDto) []Room.Room {
	if update.Id == "" {
		return nil
	}

	setClauses := ""
	args := []interface{}{}

	if update.Room.Name != "" {
		setClauses += "name = ?, "
		args = append(args, update.Room.Name)
	}
	if update.Room.Status != "" {
		setClauses += "status = ?, "
		args = append(args, string(update.Room.Status))
	}
	if setClauses == "" && len(update.Room.Members) == 0 {
		return nil
	}

	if setClauses != "" {
		setClauses += "updated_at = ?"
		args = append(args, time.Now())
		args = append(args, update.Id)
		query := "UPDATE rooms SET " + setClauses + " WHERE id = ?"
		if err := r.Session.Query(query, args...).Exec(); err != nil {
			log.Printf("ScyllaDB UpdateRoom error: %v", err)
		}
	}

	if len(update.Room.Members) > 0 {
		old := r.Session.Query(`SELECT user_id FROM room_members WHERE room_id = ?`, update.Id).Iter()
		var oldUID string
		for old.Scan(&oldUID) {
			_ = r.Session.Query(`DELETE FROM rooms_by_user WHERE user_id = ? AND room_id = ?`, oldUID, update.Id).Exec()
		}
		_ = old.Close()
		_ = r.Session.Query(`DELETE FROM room_members WHERE room_id = ?`, update.Id).Exec()

		for _, m := range update.Room.Members {
			_ = r.Session.Query(
				`INSERT INTO room_members (room_id, user_id, roles, firstname, lastname, avatar_url, notification, pin, last_seen) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				update.Id, m.Id, m.Roles, m.FirstName, m.LastName, m.AvatarUrl, m.Notification, m.Pin, m.LastSeen,
			).Exec()
			_ = r.Session.Query(
				`INSERT INTO rooms_by_user (user_id, room_id, pin, room_type) VALUES (?, ?, ?, ?)`,
				m.Id, update.Id, m.Pin, "",
			).Exec()
		}
	}

	return nil
}
