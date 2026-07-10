package ScyllaDB

import (
	"encoding/json"
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

func (r *RoomRepository) GetRoom(id string) Room.Room {
	if id == "" {
		return Room.Room{}
	}

	var room Room.Room
	var membersJSON, lastMessageJSON, status string
	var temp bool
	var createdAt, updatedAt time.Time

	err := r.Session.Query(`
		SELECT id, name, type, members_json, last_message_json, status, created_at, updated_at
		FROM rooms WHERE id = ?
	`, id).Scan(
		&room.ID, &room.Name, &temp, &membersJSON,
		&lastMessageJSON, &status, &createdAt, &updatedAt,
	)
	if err != nil {
		if err != gocql.ErrNotFound {
			log.Printf("ScyllaDB GetRoom error: %v", err)
		}
		return Room.Room{}
	}

	room.Temporary = temp
	room.Status = enums.Status(status)

	if membersJSON != "" {
		var members []ws.Member
		if err := json.Unmarshal([]byte(membersJSON), &members); err == nil {
			room.Members = members
		}
	}
	if lastMessageJSON != "" {
		var lastMsg Message.Message
		if err := json.Unmarshal([]byte(lastMessageJSON), &lastMsg); err == nil {
			room.Message = lastMsg
		}
	}

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

	query := "SELECT id, name, type, members_json, last_message_json, status FROM rooms"
	args := []interface{}{}

	if filter.Id != "" {
		query += " WHERE id = ?"
		args = append(args, filter.Id)
	}

	query += " LIMIT ? ALLOW FILTERING"
	limit := offset
	skip := (page - 1) * offset
	args = append(args, limit+skip)

	iter := r.Session.Query(query, args...).Iter()

	var rooms []Room.Room
	var room Room.Room
	var membersJSON, lastMessageJSON, status string
	var temp bool

	for iter.Scan(
		&room.ID, &room.Name, &temp, &membersJSON,
		&lastMessageJSON, &status,
	) {
		room.Temporary = temp
		room.Status = enums.Status(status)

		if membersJSON != "" {
			var members []ws.Member
			if err := json.Unmarshal([]byte(membersJSON), &members); err == nil {
				room.Members = members
			}
		}
		if lastMessageJSON != "" {
			var lastMsg Message.Message
			if err := json.Unmarshal([]byte(lastMessageJSON), &lastMsg); err == nil {
				room.Message = lastMsg
			}
		}

		rooms = append(rooms, room)
		membersJSON = ""
		lastMessageJSON = ""
		status = ""
	}

	if err := iter.Close(); err != nil {
		log.Printf("ScyllaDB GetAllRooms iter error: %v", err)
		return []Room.Room{}
	}

	if filter.MemberId != "" {
		var filtered []Room.Room
		for _, rm := range rooms {
			for _, m := range rm.Members {
				if m.Id == filter.MemberId {
					filtered = append(filtered, rm)
					break
				}
			}
		}
		rooms = filtered
	}

	if skip > 0 && len(rooms) > skip {
		rooms = rooms[skip:]
	} else if skip > 0 && len(rooms) <= skip {
		return []Room.Room{}
	}

	return rooms
}

func (r *RoomRepository) Update(update Dtos.UpdateRoomDto) []Room.Room {
	if update.Id == "" {
		return nil
	}

	var membersJSON, lastMessageJSON string
	if len(update.Room.Members) > 0 {
		if b, err := json.Marshal(update.Room.Members); err == nil {
			membersJSON = string(b)
		}
	}
	if update.Room.Message.Content != "" || !update.Room.Message.ID.IsZero() {
		if b, err := json.Marshal(update.Room.Message); err == nil {
			lastMessageJSON = string(b)
		}
	}

	setClauses := ""
	args := []interface{}{}

	if update.Room.Name != "" {
		setClauses += "name = ?, "
		args = append(args, update.Room.Name)
	}
	if membersJSON != "" {
		setClauses += "members_json = ?, "
		args = append(args, membersJSON)
	}
	if lastMessageJSON != "" {
		setClauses += "last_message_json = ?, "
		args = append(args, lastMessageJSON)
	}
	if update.Room.Status != "" {
		setClauses += "status = ?, "
		args = append(args, string(update.Room.Status))
	}

	if setClauses == "" {
		return nil
	}

	setClauses += "updated_at = ?"
	args = append(args, time.Now())
	args = append(args, update.Id)

	query := "UPDATE rooms SET " + setClauses + " WHERE id = ?"
	if err := r.Session.Query(query, args...).Exec(); err != nil {
		log.Printf("ScyllaDB UpdateRoom error: %v", err)
	}

	return nil
}
