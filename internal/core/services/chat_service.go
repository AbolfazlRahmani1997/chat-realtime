package services

import (
	"fmt"
	"os"
	"strings"

	"server/internal/core/domain"
	"server/internal/core/ports"
)

type chatService struct {
	msgRepo  ports.MessageRepository
	roomRepo ports.RoomRepository
	userRepo ports.UserRepository
	broker   ports.RoomBroker
}

func NewChatService(
	msgRepo ports.MessageRepository,
	roomRepo ports.RoomRepository,
	userRepo ports.UserRepository,
	broker ports.RoomBroker,
) ports.ChatService {
	return &chatService{
		msgRepo:  msgRepo,
		roomRepo: roomRepo,
		userRepo: userRepo,
		broker:   broker,
	}
}

func (s *chatService) SendMessage(roomID string, msg *domain.Message) error {
	if msg.UniqId == "" {
		return fmt.Errorf("message uniqid is required")
	}
	return s.msgRepo.InsertMessage(msg)
}

func (s *chatService) GetRoomMessages(roomID string, page int) ([]domain.Message, error) {
	return s.msgRepo.GetRoomMessages(roomID, page, 10)
}

func (s *chatService) MarkAsRead(messageID, userID string) (*domain.Message, error) {
	msg, err := s.msgRepo.GetMessageByID(messageID)
	if err != nil {
		msg, err = s.msgRepo.GetMessageByUniqId(messageID)
	}
	if err != nil || msg == nil {
		return nil, fmt.Errorf("message not found")
	}
	msg.Read = append(msg.Read, userID)
	if err := s.msgRepo.UpdateReadReceipt(msg.ID, msg.Read); err != nil {
		return nil, err
	}
	return msg, nil
}

func (s *chatService) DeliverMessage(roomID, userID string) error {
	messages, err := s.msgRepo.GetUndeliveredMessages(roomID, userID)
	if err != nil {
		return err
	}
	for i := range messages {
		messages[i].Deliver = append(messages[i].Deliver, userID)
		if err := s.msgRepo.UpdateDelivery(messages[i].ID, messages[i].Deliver); err != nil {
			return err
		}
	}
	return nil
}

func (s *chatService) SyncRoomMembers(room *domain.Room) (*domain.Room, error) {
	for i, m := range room.Members {
		user, err := s.userRepo.GetByID(m.Id)
		if err == nil && user != nil {
			room.Members[i].FirstName = user.FirstName
			room.Members[i].LastName = user.LastName
			room.Members[i].AvatarUrl = user.AvatarUrl
		}
	}
	if err := s.roomRepo.UpdateMembers(room.ID, room.Members); err != nil {
		return nil, err
	}
	return room, nil
}

const supportAdminID = ""

func (s *chatService) createAutoRoom(userID, roomID, roomName string, roomType domain.RoomType, extraMembers []domain.Member) error {
	existing, _ := s.roomRepo.GetByID(roomID)
	if existing != nil {
		return nil
	}

	members := []domain.Member{
		{Id: userID, Roles: []string{"Owner", "Writer"}},
	}
	members = append(members, extraMembers...)

	room := &domain.Room{
		ID:      roomID,
		Name:    roomName,
		Type:    roomType,
		Members: members,
		Status:  domain.Offline,
	}
	return s.roomRepo.Save(room)
}

func (s *chatService) CreateSupportRoom(userID string) error {
	admins := []domain.Member{{Id: supportAdminID, Roles: []string{"Writer"}}}
	adminsEnv := os.Getenv("SUPPORT_ADMIN_IDS")
	if adminsEnv != "" {
		for _, raw := range strings.Split(adminsEnv, ",") {
			id := strings.TrimSpace(raw)
			if id == "" || id == supportAdminID || id == userID {
				continue
			}
			admins = append(admins, domain.Member{Id: id, Roles: []string{"Writer"}})
		}
	}
	return s.createAutoRoom(userID, fmt.Sprintf("support-%s", userID), "پشتیبانی", domain.Support, admins)
}

func (s *chatService) CreateBotRoom(userID string) error {
	return s.createAutoRoom(userID, fmt.Sprintf("bot-%s", userID), "Ai Assistant", domain.BOT,
		[]domain.Member{{Id: "bot", Roles: []string{"Writer"}}})
}

func (s *chatService) GetUserRooms(userID, page, filterType string) ([]domain.Room, error) {
	pageInt := 1
	if p := page; p != "" {
		fmt.Sscanf(p, "%d", &pageInt)
	}

	roomList, err := s.roomRepo.GetUserRooms(userID, pageInt, 10, filterType)
	if err != nil {
		return nil, err
	}

	hasSupport := false
	hasBot := false
	for _, r := range roomList {
		if strings.HasPrefix(r.ID, "support-") {
			hasSupport = true
		}
		if r.Type == domain.BOT {
			hasBot = true
		}
	}

	if userID != supportAdminID && !hasSupport {
		s.CreateSupportRoom(userID)
	}
	if !hasBot {
		s.CreateBotRoom(userID)
	}

	return roomList, nil
}

func (s *chatService) TogglePin(roomID, userID string) (*domain.Specification, error) {
	return s.updateSpec(roomID, userID, true, false)
}

func (s *chatService) ToggleNotification(roomID, userID string) (*domain.Specification, error) {
	return s.updateSpec(roomID, userID, false, true)
}

func (s *chatService) updateSpec(roomID, userID string, togglePin, toggleNotif bool) (*domain.Specification, error) {
	room, err := s.roomRepo.GetByID(roomID)
	if err != nil {
		return nil, err
	}

	spec := &domain.Specification{}
	for i, m := range room.Members {
		if m.Id == userID {
			if toggleNotif {
				room.Members[i].Notification = !m.Notification
				spec.Notification = room.Members[i].Notification
			}
			if togglePin {
				if !m.Pin {
					pinned, _ := s.roomRepo.GetPinnedRooms(userID, 5)
					if len(pinned) < 5 {
						room.Members[i].Pin = true
					}
				} else {
					room.Members[i].Pin = false
				}
				spec.Pin = room.Members[i].Pin
			}
			break
		}
	}

	if err := s.roomRepo.UpdateMembers(roomID, room.Members); err != nil {
		return nil, err
	}
	return spec, nil
}

func (s *chatService) GetUser(userID string) (*domain.User, error) {
	return s.userRepo.GetByID(userID)
}

func (s *chatService) UpdateUser(user *domain.User) error {
	return s.userRepo.Upsert(user)
}
