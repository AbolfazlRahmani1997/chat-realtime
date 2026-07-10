package ws

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	"server/internal/message_broker"

	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type Member struct {
	Id           string   `json:"Id"`
	Roles        []string `bson:"roles" json:"roles"`
	FirstName    string   `json:"firstname"`
	LastName     string   `json:"lastname"`
	AvatarUrl    string   `json:"AvatarUrl"bson:"avatar_url"`
	Notification bool     `json:"Notification"`
	Pin          bool     `json:"Pin"`
	LastSeen     int64    `json:"LastSeen,omitempty" bson:"last_seen"`
}

type ReadMessage struct {
	MessageId string `json:"MessageId"`
	UserId    string `json:"UserId"`
	RoomID    string `json:"RoomID"`
}

type RoomStatus struct {
	RoomId  string `json:"roomId"`
	Status  status `json:"status"`
	Message string `json:"Message"`
}

type RoomModel struct {
	ID           primitive.ObjectID `json:"_id"`
	RoomId       string             `json:"id"`
	Name         string             `json:"name" `
	Type         Type               `json:"type" `
	AvatarUrl    string             `json:"avatar_url" bson:"avatar_url"`
	Members      []Member           `json:"members"`
	Message      Message            `json:"message" bson:"last_message"`
	Status       status             `json:"status" `
	ActiveStatus string             `json:"active_status" bson:"active_status"`
	Clients      map[string]*Client `json:"clients"`
}
type Type string

const (
	PRIVATE Type = "PRIVATE"
	GROUP   Type = "GROUP"
	Support Type = "SUPPORT"
	TEMP    Type = "TEMP"
	CHANEL  Type = "CHANEL"
	BOT     Type = "BOT"
)

type Room struct {
	_Id          primitive.ObjectID `json:"_id"`
	ID           string             `json:"Id" `
	Name         string             `json:"Name" `
	Type         Type               `json:"Type" `
	AvatarUrl    string             `json:"AvatarUrl" bson:"avatar_url"`
	Members      []Member           `bson:"members" json:"members"`
	Message      Message            `json:"Message" bson:"last_message"`
	Status       status             `json:"Status,omitempty" `
	ActiveStatus string             `json:"ActiveStatus,omitempty" bson:"active_status"`
	Clients      map[string]*Client `json:"Clients"`
	Pinned       bool               `json:"Pinned" bson:"pinned"`
}

type RoomTemp struct {
	_Id          primitive.ObjectID `json:"_id"`
	ID           string             `json:"Id" `
	Name         string             `json:"Name" `
	Temporary    bool               `json:"Type" `
	Members      Member             `json:"Members"`
	Message      Message            `json:"Message" bson:"last_message"`
	Status       status             `json:"Status,omitempty" `
	ActiveStatus string             `json:"ActiveStatus,omitempty" bson:"active_status"`
	Clients      map[string]*Client `json:"Clients"`
}

type AddConnectionRequest struct {
	UserId       string
	ConnectionId string
	Conn         ConnWriter
}

type Hub struct {
	mx                   sync.RWMutex
	Users                map[string]*User
	Rooms                map[string]*Room
	Register             chan *Client
	ReadAble             chan *ReadMessage
	Join                 chan *User
	AddConnection        chan *AddConnectionRequest
	AddUnifiedConnection chan *AddConnectionRequest
	Left                 chan *UserConnection
	Evade                chan *User
	Unregister           chan *Client
	Broadcast            chan *Message
	Room                 chan *Room
	MessageService       MessageService
	RoomService          RoomService
	RoomBroker           RoomBrokerInfrastructure
	RedisBroker          *RedisPubSubBroker
	PresenceManager      *RedisPresenceManager
	DB                   DatabaseProvider
	Ntfy                 *NtfyService
	KafkaProducer        *messagebroker.Producer
}

func NewHub(client *mongo.Client, redisClient *redis.Client, db DatabaseProvider, kafkaProducer *messagebroker.Producer) *Hub {

	var messageRepository MessageRepository
	var userRepository UserRepository
	var RoomRepository RoomMongoRepository
	var RoomService RoomService
	var service MessageService

	clientDatabase := (*mongo.Database)(nil)
	if client != nil {
		clientDatabase = client.Database(os.Getenv("CHAT_DB"))
		messageRepository = NewMessageRepository(clientDatabase)
		userRepository = NewUserRepository(clientDatabase)
		RoomRepository = NewRoomRepository(clientDatabase)
	}
	RoomService = NewRoomService(RoomRepository, messageRepository, userRepository, db)
	service = MessageService{messageRepository, db}
	roomChan := make(chan *Room)

	hub := &Hub{
		Rooms:                make(map[string]*Room),
		Register:             make(chan *Client),
		ReadAble:             make(chan *ReadMessage),
		Unregister:           make(chan *Client),
		Broadcast:            make(chan *Message, 100),
		Join:                 make(chan *User),
		AddConnection:        make(chan *AddConnectionRequest),
		AddUnifiedConnection: make(chan *AddConnectionRequest),
		Left:                 make(chan *UserConnection),
		Evade:                make(chan *User),
		Users:                make(map[string]*User),
		Room:                 roomChan,
		MessageService:       service,
		RoomService:          RoomService,
		DB:                   db,
		KafkaProducer:        kafkaProducer,
	}

	hub.Ntfy = NewNtfyService()

	if redisClient != nil {
		redisBroker := NewRedisPubSubBroker(hub, redisClient)
		hub.RedisBroker = redisBroker
		hub.PresenceManager = NewRedisPresenceManager(hub, redisBroker, redisClient)
		redisBroker.Start()
	}

	rabbitEnabled := os.Getenv("RABBITMQ_ENABLED")
	if rabbitEnabled == "" {
		rabbitEnabled = "true"
	}

	if rabbitEnabled != "false" {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("rabbitmq consumer panic: %v", r)
				}
			}()
			mqBroker := NewRabbitMqBroker(roomChan, messageRepository, db)
			mqBroker.Consume()
		}()
	} else {
		log.Println("RabbitMQ is disabled; running chat service without RabbitMQ integration")
	}

	return hub

}

func (h *Hub) Run() {
	defer func() {
	}()

	for {
		select {
		case messageId := <-h.ReadAble:
			go func() {
				message := h.MessageService.MessageRead(messageId.MessageId, messageId.UserId)
				h.mx.RLock()
				user, ok := h.Users[message.ClientID]
				h.mx.RUnlock()
				if ok && user != nil {
					select {
					case user.seenMessage <- &SeenNotification{MessageId: message.UniqId, RoomId: message.RoomID}:
					default:
					}
				}
				if h.RedisBroker != nil && message.RoomID != "" {
					h.RedisBroker.PublishReadReceipt(&PubSubReadReceipt{
						MessageID: messageId.MessageId,
						UserID:    messageId.UserId,
						RoomID:    message.RoomID,
					})
				}
			}()
		case room := <-h.Room:
			{
				h.Rooms[room.ID] = &Room{
					_Id:     room._Id,
					ID:      room.ID,
					Name:    room.Name,
					Members: room.Members,
					Clients: make(map[string]*Client),
				}
				if room._Id.IsZero() {
					SyncedRoom := h.RoomService.SyncUser(*room)
					for _, member := range room.Members {
						if user, ok := h.Users[member.Id]; ok {

							if user.IsConnected {
								go func() {
									user.createRoom <- &SyncedRoom
								}()
							}
						}
					}
				}

				if h.RedisBroker != nil {
					h.RedisBroker.PublishRoomEvent(&PubSubRoomEvent{
						Type:     "room_created",
						RoomID:   room.ID,
						RoomData: h.Rooms[room.ID],
					})
				}

				if h.Ntfy != nil && room._Id.IsZero() {
					for _, member := range room.Members {
						if member.Id != "" {
							go h.Ntfy.NotifyNewRoom(member.Id, room.Name)
						}
					}
				}

			}

		case cl := <-h.Register:
			if _, ok := h.Rooms[cl.RoomID]; ok {
				r := h.Rooms[cl.RoomID]
				if client, ok := r.Clients[cl.ID]; ok {
					cl.Conn = mergeConnection(cl.Conn, client.Conn)
					cl.Message = make(chan *Message)
					cl.Status = online

				} else {
					r.Clients[cl.ID] = cl
				}
				if !cl.UnifiedConn {
					for s := range cl.Conn {
						go cl.readerMessage(s, h)
					}
				}
				r.Clients[cl.ID] = cl
				room := h.Rooms[cl.RoomID]
				room.Status = online
				go func() {
					members := h.Rooms[cl.RoomID].Members
					for _, member := range members {
						if user, ok := h.Users[member.Id]; ok {
							if cl.ID != user.UserId {
								go func() {
									user.roomStatuses <- &RoomStatus{
										RoomId: h.Rooms[cl.RoomID].ID,
										Status: online,
									}
								}()
							}

						}
					}

				}()

			}
		case cl := <-h.Unregister:
			if room, ok := h.Rooms[cl.RoomID]; ok {
				if _, ok := room.Clients[cl.ID]; ok {
					close(cl.Message)
					delete(room.Clients, cl.ID)
				}
				members := room.Members
				if len(room.Clients) == 0 {
					room.Status = offline
					h.RoomService.changeRoomStatus(*room)
					delete(h.Rooms, cl.RoomID)
				}

				for _, member := range members {
					if user, ok := h.Users[member.Id]; ok {
						go func(u *User, roomId string) {
							u.roomStatuses <- &RoomStatus{
								RoomId: roomId,
								Status: offline,
							}
						}(user, room.ID)
					}
				}

			}
		case m := <-h.Broadcast:

			go func() {
				isRemote := m.ID != ""
				m.CreatedAt = time.Now()
				if _, ok := h.Rooms[m.RoomID]; ok {
					if m.ID == "" {
						m.Deliver = nil
						m.Read = nil
						m.ID = m.UniqId

						if h.KafkaProducer != nil {
							payload := &messagebroker.MessagePayload{
								ID:          m.ID,
								Content:     m.Content,
								Type:        m.Type,
								UniqId:      m.UniqId,
								RoomID:      m.RoomID,
								Username:    m.Username,
								ClientID:    m.ClientID,
								Deliver:     m.Deliver,
								Read:        m.Read,
								CreatedAt:   m.CreatedAt,
								UpdatedAt:   m.UpdatedAt,
							}
							if m.ReplyTo != nil {
								b, _ := json.Marshal(m.ReplyTo)
								var ri messagebroker.ReplyInfo
								json.Unmarshal(b, &ri)
								payload.ReplyTo = &ri
							}
							if m.ForwardFrom != nil {
								b, _ := json.Marshal(m.ForwardFrom)
								var fi messagebroker.ForwardInfo
								json.Unmarshal(b, &fi)
								payload.ForwardFrom = &fi
							}
							if err := h.KafkaProducer.PublishMessage(payload); err != nil {
								log.Printf("kafka publish failed, falling back to direct DB write: %v", err)
								h.DB.MessageOps().InsertMessage(*m)
							}
						} else {
							h.DB.MessageOps().InsertMessage(*m)
						}
					}
					h.RoomService.UpdateLastMessage(*h.Rooms[m.RoomID], *m)

					if h.RedisBroker != nil && !isRemote {
						h.RedisBroker.PublishMessage(m.RoomID, m)
					}

					members := h.Rooms[m.RoomID].Members
					var firstname string
					var lastname string
					for _, userID := range members {

						if userID.Id == m.ClientID {

							firstname = userID.FirstName
							lastname = userID.LastName
						}

						if user, ok := h.Users[userID.Id]; ok {
							if _, ok := h.Rooms[m.RoomID].Clients[user.UserId]; !ok {
								if user.UserId != m.ClientID {

									go func() {
										user.pupMessage <- &PupMessage{
											MessageId: m.ID,
											RoomId:    m.RoomID,
											Firstname: firstname,
											Lastname:  lastname,
											Content:   m.Content,
											Type:      m.Type,
										}
									}()

								}
							}

						}

					}
					if h.Ntfy != nil && m.ClientID != "" {
						senderName := firstname
						if lastname != "" {
							senderName += " " + lastname
						}
						if senderName == "" || senderName == " " {
							senderName = m.Username
						}
						if senderName == "" {
							senderName = "کاربر"
						}
						roomClients := h.Rooms[m.RoomID].Clients
						for _, userID := range members {
							if userID.Id == m.ClientID {
								continue
							}
							_, inRoom := roomClients[userID.Id]
							if !inRoom {
								go h.Ntfy.NotifyNewMessage(userID.Id, senderName, m.Content, m.RoomID, m.Type, "")
							}
						}
					}
					for _, cl := range h.Rooms[m.RoomID].Clients {

						if ok := cl.Status == online; ok {
							if cl.ID != m.ClientID {
								m.Deliver = append(m.Deliver, cl.ID)
								h.DB.MessageOps().MessageDelivery(m.ID, m.Deliver)
							}
							select {
							case cl.Message <- m:
							default:
							}

						}

					}

				}

			}()
		}

	}
}

func (h *Hub) Manager() {
	for {
		select {
		case req := <-h.AddConnection:
			h.mx.Lock()
			if userExists, ok := h.Users[req.UserId]; ok {
				userExists.Conn[req.ConnectionId] = req.Conn
				if h.PresenceManager != nil {
					h.PresenceManager.AddUserConnection(req.UserId, req.ConnectionId)
				}
				go userExists.userConnection(h, req.ConnectionId)
			}
			h.mx.Unlock()
		case req := <-h.AddUnifiedConnection:
			h.mx.Lock()
			isNewUser := false
			if userExists, ok := h.Users[req.UserId]; ok {
				userExists.Conn[req.ConnectionId] = req.Conn
			} else {
				isNewUser = true
				user := &User{
					Conn:   map[string]ConnWriter{req.ConnectionId: req.Conn},
					UserId: req.UserId,
					online: true,
				}
				user.roomStatuses = make(chan *RoomStatus)
				user.pupMessage = make(chan *PupMessage)
				user.chanelNotification = make(chan *SystemMessage)
				user.createRoom = make(chan *Room)
				user.seenMessage = make(chan *SeenNotification)
				go user.WireRooms(h)
				h.Users[req.UserId] = user
			}
			if h.PresenceManager != nil {
				h.PresenceManager.AddUserConnection(req.UserId, req.ConnectionId)
			}
			if isNewUser {
				go h.OnlineMessage(req.UserId, online)
			}
			h.mx.Unlock()
		case user, _ := <-h.Join:

			h.mx.Lock()
			if userExists, ok := h.Users[user.UserId]; ok {
				userExists.Conn = mergeConnection(userExists.Conn, user.Conn)
				for s := range user.Conn {
					if h.PresenceManager != nil {
						h.PresenceManager.AddUserConnection(user.UserId, s)
					}
					go userExists.userConnection(h, s)
				}
			} else {
				user.roomStatuses = make(chan *RoomStatus)
				user.pupMessage = make(chan *PupMessage)
				user.chanelNotification = make(chan *SystemMessage)
				user.createRoom = make(chan *Room)
				user.seenMessage = make(chan *SeenNotification)
				go user.WireRooms(h)
				h.Users[user.UserId] = user
				for s := range user.Conn {
					if h.PresenceManager != nil {
						h.PresenceManager.AddUserConnection(user.UserId, s)
					}
					go user.userConnection(h, s)
				}
				go h.OnlineMessage(user.UserId, online)
			}
			h.mx.Unlock()
		case uc, _ := <-h.Left:
			h.mx.Lock()
			u, exists := h.Users[uc.Userid]
			if exists && u != nil {
				delete(u.Conn, uc.ConnectionId)
				if h.PresenceManager != nil {
					h.PresenceManager.RemoveUserConnection(uc.Userid, uc.ConnectionId)
				}
				if len(u.Conn) == 0 {
					now := time.Now().Unix()
					go func() {
						h.DB.UserOps().UpdateLastSeen(uc.Userid, now)
						h.updateMembersLastSeen(uc.Userid, now)
					}()
					delete(h.Users, uc.Userid)
					go h.OnlineMessage(uc.Userid, offline)
				}
			}
			h.mx.Unlock()

		case user, _ := <-h.Evade:
			go h.OnlineMessage(user.UserId, evade)
		}

	}
}

func (h *Hub) OnlineMessage(userId string, status status) {
	if h.DB == nil {
		return
	}
	rooms := h.DB.RoomOps().GetOlineMyRooms(userId)
	for _, room := range rooms {
		for _, client := range room.Members {
			if client.Id != userId {
				if user, ok := h.Users[client.Id]; ok {
					user.roomStatuses <- &RoomStatus{
						RoomId: room.ID,
						Status: status,
					}
				}
			}

		}
	}

	if h.RedisBroker != nil && h.PresenceManager != nil {
		if status == online {
			h.PresenceManager.BroadcastUserOnline(userId, rooms)
		} else if status == offline {
			h.PresenceManager.BroadcastUserOffline(userId, rooms)
		}
	}

}

func (h *Hub) updateMembersLastSeen(userId string, lastSeen int64) {
	if h.DB == nil {
		return
	}
	rooms := h.DB.RoomOps().GetOlineMyRooms(userId)
	for _, room := range rooms {
		for i, member := range room.Members {
			if member.Id == userId {
				room.Members[i].LastSeen = lastSeen
				break
			}
		}
		h.DB.RoomOps().UpdateMember(room)
		if r, ok := h.Rooms[room.ID]; ok {
			for i, m := range r.Members {
				if m.Id == userId {
					r.Members[i].LastSeen = lastSeen
					break
				}
			}
		}
	}
}

func mergeConnection(m1 map[string]ConnWriter, m2 map[string]ConnWriter) map[string]ConnWriter {
	merged := make(map[string]ConnWriter)
	for k, v := range m1 {
		merged[k] = v
	}
	for key, value := range m2 {
		merged[key] = value
	}
	return merged
}
