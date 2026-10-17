package ws

import (
	"strconv"
)

const supportAdminID = ""

type RoomService struct {
	RoomRepository    RoomMongoRepository
	MemberRepository  UserRepository
	MessageRepository MessageRepository
	DB                DatabaseProvider
}

type RoomResponse struct {
	Id                string   `json:"Id,omitempty"`
	Name              string   `json:"Name"`
	Members           []Member `json:"Members"`
	AvatarUrl         string   `json:"AvatarUrl"`
	Status            status   `json:"Status"`
	NotDeliverMessage int64    `json:"CountMessages"`
	LastMessage       Message  `json:"Message"`
	Type              Type     `json:"Type"`
}

// todo retrun  Get MyRome With Type
func (r RoomService) GetMyRoom(userId string, page string, filterType string) []RoomResponse {
	var Rooms []RoomResponse

	pageA, _ := strconv.Atoi(page)
	roomList := r.DB.RoomOps().GetMyRooms(userId, pageA, 10, filterType)

	for _, room := range roomList {
		// Member profiles are stored on the room. Syncing every member and
		// writing the room back on each list call does not survive a login storm.
		members := room.Members
		if room.Type == PRIVATE && r.DB != nil {
			members = append([]Member(nil), room.Members...)
			for i := range members {
				if members[i].Id == userId {
					continue
				}
				profile := r.DB.UserOps().GetUser(members[i].Id)
				if profile.Id == "" {
					continue
				}
				members[i].FirstName = profile.FirstName
				members[i].LastName = profile.LastName
				if profile.AvatarUrl != "" {
					members[i].AvatarUrl = profile.AvatarUrl
				}
			}
		}
		notDelivered := r.DB.MessageOps().GetMessageNotCountDelivery(room.ID, userId)
		Rooms = append(Rooms, RoomResponse{
			Id:                room.ID,
			Name:              room.Name,
			Members:           members,
			Status:            room.Status,
			AvatarUrl:         room.AvatarUrl,
			NotDeliverMessage: notDelivered,
			LastMessage:       room.Message,
			Type:              room.Type,
		})
	}

	// اگر برای این کاربر هنوز اتاق پشتیبانی وجود ندارد، به صورت خودکار یک اتاق support-<userId> بساز
	return Rooms
}

func (r RoomService) UpdateLastMessage(room Room, message Message) {
	r.DB.RoomOps().LastMessage(room.ID, message)
}

func NewRoomService(RoomRepository RoomMongoRepository, MessageRepository MessageRepository, UserRepository UserRepository, db DatabaseProvider) RoomService {
	return RoomService{
		RoomRepository:    RoomRepository,
		MemberRepository:  UserRepository,
		MessageRepository: MessageRepository,
		DB:                db,
	}
}
func (receiver RoomService) SyncUser(room Room) Room {
	var newMember []Member
	for _, m := range room.Members {
		member := receiver.DB.UserOps().GetUser(m.Id)

		m.FirstName = member.FirstName
		m.LastName = member.LastName
		m.AvatarUrl = member.AvatarUrl
		newMember = append(newMember, m)

	}
	room.Members = newMember
	go receiver.DB.RoomOps().UpdateMember(room)
	return room
}

type SpecificationRoom struct {
	Notification bool `json:"Notification"`
	Pin          bool `json:"Pin"`
}

func (receiver RoomService) updateRoomSpecification(id string, userId string, notification SpecificationRoom) []Member {
	var NewMember []Member
	room := receiver.DB.RoomOps().GetById(id)
	member := room.Members
	var LastStatus SpecificationRoom

	for _, m := range member {

		if m.Id == userId {
			if notification.Notification == true {
				m.Notification = !m.Notification
				LastStatus.Notification = m.Notification
			}
			if notification.Pin == true {
				if !m.Pin {
					pinnedRooms := receiver.DB.RoomOps().GetMyPinRooms(userId)
					if len(pinnedRooms) <= 5 {
						m.Pin = !m.Pin
					}
				} else {
					m.Pin = !m.Pin
				}

				LastStatus.Pin = m.Pin
			}

		}
		NewMember = append(NewMember, m)
	}

	room.Members = NewMember
	receiver.DB.RoomOps().UpdateMember(room)
	return NewMember
}

func (r RoomService) changeRoomStatus(room Room) {
	r.DB.RoomOps().Update(room)
}
