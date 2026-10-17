package ws

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"sync/atomic"
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

const (
	hubBroadcastBuffer  = 4096
	hubBroadcastWorkers = 32
	hubControlBuffer    = 256
)

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
	persistCh            chan persistJob
	roomWatchers         map[string]map[string]struct{}
	userRooms            map[string]map[string]struct{}
	shardRooms           map[int]int
	workerOnce           sync.Once
	liveConns            atomic.Uint64
	MessageService       MessageService
	RoomService          RoomService
	RoomBroker           RoomBrokerInfrastructure
	RedisBroker          *RedisPubSubBroker
	PresenceManager      *RedisPresenceManager
	DB                   DatabaseProvider
	Ntfy                 *NtfyService
	KafkaProducer        *messagebroker.Producer
	droppedBroadcasts    atomic.Uint64
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
	roomChan := make(chan *Room, hubControlBuffer)

	hub := &Hub{
		Rooms:                make(map[string]*Room),
		Register:             make(chan *Client, hubControlBuffer),
		ReadAble:             make(chan *ReadMessage, hubControlBuffer),
		Unregister:           make(chan *Client, hubControlBuffer),
		Broadcast:            make(chan *Message, hubBroadcastBuffer),
		Join:                 make(chan *User, hubControlBuffer),
		AddConnection:        make(chan *AddConnectionRequest, hubControlBuffer),
		AddUnifiedConnection: make(chan *AddConnectionRequest, hubControlBuffer),
		Left:                 make(chan *UserConnection, hubControlBuffer),
		Evade:                make(chan *User, hubControlBuffer),
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
	h.ensureScaleWorkers()
	for {
		select {
		case room := <-h.Room:
			h.handleRoomCreated(room)
		case cl := <-h.Register:
			h.handleRegister(cl)
		case cl := <-h.Unregister:
			h.handleUnregister(cl)
		}
	}
}

func (h *Hub) broadcastWorker() {
	for m := range h.Broadcast {
		h.handleBroadcast(m)
	}
}

// EnqueueBroadcast queues a message for fan-out without blocking callers forever.
func (h *Hub) EnqueueBroadcast(m *Message) bool {
	if m == nil {
		return false
	}
	select {
	case h.Broadcast <- m:
		return true
	default:
		n := h.droppedBroadcasts.Add(1)
		// Rate-limit logs under load (every 10k drops).
		if n == 1 || n%10000 == 0 {
			log.Printf("hub: broadcast queue full (dropped=%d) room=%s uniq=%s", n, m.RoomID, m.UniqId)
		}
		return false
	}
}

func (h *Hub) handleReadReceipt(messageId *ReadMessage) {
	if messageId == nil {
		return
	}
	message := h.MessageService.MessageRead(messageId.MessageId, messageId.UserId)
	if message.ID == "" && message.UniqId == "" {
		return
	}
	roomID := message.RoomID
	if roomID == "" {
		roomID = messageId.RoomID
	}
	msgKey := message.UniqId
	if msgKey == "" {
		msgKey = message.ID
	}
	// Notify the message author (not the reader).
	authorID := message.ClientID
	if authorID != "" && authorID != messageId.UserId {
		if user, ok := h.getUserLocked(authorID); ok && user != nil {
			user.trySendSeen(&SeenNotification{MessageId: msgKey, RoomId: roomID})
		}
	}
	if h.RedisBroker != nil && roomID != "" {
		h.RedisBroker.PublishReadReceipt(&PubSubReadReceipt{
			MessageID: msgKey,
			UserID:    messageId.UserId,
			AuthorID:  authorID,
			RoomID:    roomID,
		})
	}
}

// fanoutReaction pushes the current reaction set to everyone joined in the room.
func (h *Hub) fanoutReaction(roomID, messageID string, reactions []Reaction) {
	if roomID == "" || messageID == "" {
		return
	}
	if reactions == nil {
		reactions = []Reaction{}
	}
	payload := map[string]interface{}{
		"event_type": "messageReaction",
		"room_id":    roomID,
		"content": map[string]interface{}{
			"message_id": messageID,
			"reactions":  reactions,
		},
	}
	snap, ok := h.snapshotRoomForBroadcast(roomID)
	if !ok {
		return
	}
	for _, cl := range snap.clients {
		if cl != nil {
			cl.writeJSONAll(payload)
		}
	}
}

// fanoutEdit tells everyone in the room that a message text changed.
func (h *Hub) fanoutEdit(roomID, messageID, content string) {
	if roomID == "" || messageID == "" {
		return
	}
	payload := map[string]interface{}{
		"event_type": "messageEdited",
		"room_id":    roomID,
		"content": map[string]interface{}{
			"message_id": messageID,
			"content":    content,
			"edited":     true,
		},
	}
	snap, ok := h.snapshotRoomForBroadcast(roomID)
	if !ok {
		return
	}
	for _, cl := range snap.clients {
		if cl != nil {
			cl.writeJSONAll(payload)
		}
	}
}

// fanoutSecret tells the room that a secret was opened or wiped. It never includes the text.
func (h *Hub) fanoutSecret(roomID, eventType, messageID, userID string) {
	if roomID == "" || messageID == "" || eventType == "" {
		return
	}
	payload := map[string]interface{}{
		"event_type": eventType,
		"room_id":    roomID,
		"content": map[string]string{
			"message_id": messageID,
			"user_id":    userID,
		},
	}
	snap, ok := h.snapshotRoomForBroadcast(roomID)
	if !ok {
		return
	}
	for _, cl := range snap.clients {
		if cl != nil {
			cl.writeJSONAll(payload)
		}
	}
}

// notifyDelivered tells the author that another member now has the message.
// Content is an object so clients can tell it apart from the send ack, which uses a string ulid.
func (h *Hub) notifyDelivered(authorID, messageID, roomID, userID string) {
	if authorID == "" || messageID == "" || authorID == userID {
		return
	}
	user, ok := h.getUserLocked(authorID)
	if !ok || user == nil {
		return
	}
	user.dispatch(&SystemMessage{
		EventType: deliverMessage,
		Content: map[string]string{
			"message_id": messageID,
			"room_id":    roomID,
			"user_id":    userID,
		},
	})
}

func (h *Hub) handleRoomCreated(room *Room) {
	if room == nil || room.ID == "" {
		return
	}
	h.mx.Lock()
	cached := &Room{
		_Id:       room._Id,
		ID:        room.ID,
		Name:      room.Name,
		Type:      room.Type,
		AvatarUrl: room.AvatarUrl,
		Members:   append([]Member(nil), room.Members...),
		Status:    room.Status,
		Clients:   make(map[string]*Client),
	}
	if existing, ok := h.Rooms[room.ID]; ok && existing != nil {
		// Keep live clients if room was EnsureRoom'd before notify path.
		cached.Clients = existing.Clients
		if cached.Status == "" {
			cached.Status = existing.Status
		}
	}
	h.Rooms[room.ID] = cached
	members := append([]Member(nil), room.Members...)
	roomID := room.ID
	roomName := room.Name
	isNew := room._Id.IsZero()
	var candidateUsers []*User
	if isNew {
		for _, member := range members {
			if user, ok := h.Users[member.Id]; ok && user != nil {
				candidateUsers = append(candidateUsers, user)
			}
		}
	}
	roomData := *cached
	roomData.Clients = nil
	h.mx.Unlock()

	// Check connection state outside h.mx to avoid lock-order issues with User.mx.
	var notifyUsers []*User
	for _, user := range candidateUsers {
		if user.hasConnections() {
			notifyUsers = append(notifyUsers, user)
		}
	}

	// Heavy I/O must not block the Hub control loop.
	go func() {
		if isNew {
			synced := h.RoomService.SyncUser(*room)
			for _, user := range notifyUsers {
				user.trySendCreateRoom(&synced)
			}
		}
		if h.RedisBroker != nil {
			h.RedisBroker.PublishRoomEvent(&PubSubRoomEvent{
				Type:     "room_created",
				RoomID:   roomID,
				RoomData: &roomData,
			})
		}
		if h.Ntfy != nil && isNew {
			for _, member := range members {
				if member.Id != "" {
					h.Ntfy.NotifyNewRoom(member.Id, roomName)
				}
			}
		}
	}()
}

func (h *Hub) handleRegister(cl *Client) {
	if cl == nil {
		return
	}
	h.mx.Lock()
	room, ok := h.Rooms[cl.RoomID]
	if !ok || room == nil {
		h.mx.Unlock()
		cl.shutdown()
		return
	}
	if existing, exists := room.Clients[cl.ID]; exists && existing != nil && existing != cl {
		// Legacy multi-conn rooms may merge sockets; unified WS must not inherit stale conns.
		if !cl.UnifiedConn {
			existing.mu.Lock()
			oldConns := mergeConnection(nil, existing.Conn)
			existing.mu.Unlock()
			cl.mu.Lock()
			cl.Conn = mergeConnection(cl.Conn, oldConns)
			cl.mu.Unlock()
		}
		existing.shutdown()
	}
	if cl.Message == nil {
		cl.Message = newClientMessageChan()
	}
	cl.setStatus(online)
	room.Clients[cl.ID] = cl
	room.Status = online
	members := append([]Member(nil), room.Members...)
	roomID := room.ID
	clientID := cl.ID
	unified := cl.UnifiedConn
	var connKeys []string
	if !unified {
		cl.mu.Lock()
		for s := range cl.Conn {
			connKeys = append(connKeys, s)
		}
		cl.mu.Unlock()
	}
	var notifyUsers []*User
	for _, member := range members {
		if user, ok := h.Users[member.Id]; ok && user != nil && clientID != user.UserId {
			notifyUsers = append(notifyUsers, user)
		}
	}
	h.mx.Unlock()

	if !unified {
		for _, s := range connKeys {
			go cl.readerMessage(s, h)
		}
	}
	for _, user := range notifyUsers {
		user.trySendRoomStatus(&RoomStatus{RoomId: roomID, Status: online})
	}
}

func (h *Hub) handleUnregister(cl *Client) {
	if cl == nil {
		return
	}
	h.mx.Lock()
	room, ok := h.Rooms[cl.RoomID]
	if !ok || room == nil {
		h.mx.Unlock()
		cl.shutdown()
		return
	}
	existing, exists := room.Clients[cl.ID]
	if !exists || existing != cl {
		// Stale unregister after reconnect replaced this client.
		h.mx.Unlock()
		if existing != cl {
			cl.shutdown()
		}
		return
	}
	delete(room.Clients, cl.ID)
	members := append([]Member(nil), room.Members...)
	roomID := room.ID
	roomEmpty := len(room.Clients) == 0
	var notifyUsers []*User
	if roomEmpty {
		room.Status = offline
		// Drop local cache only. Do NOT persist offline — other instances may still have clients.
		delete(h.Rooms, cl.RoomID)
		for _, member := range members {
			if user, ok := h.Users[member.Id]; ok && user != nil {
				notifyUsers = append(notifyUsers, user)
			}
		}
	}
	h.mx.Unlock()

	cl.shutdown()

	// Only when this node has no clients left in the room.
	for _, user := range notifyUsers {
		user.trySendRoomStatus(&RoomStatus{RoomId: roomID, Status: offline})
	}
}

func (h *Hub) handleBroadcast(m *Message) {
	if m == nil {
		return
	}
	isRemote := m.remote
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now()
	}

	snap, ok := h.snapshotRoomForBroadcast(m.RoomID)
	if !ok {
		// A remote message is only worth a DB read when a local user watches the room.
		// Otherwise every pod would query Mongo for every message in the cluster.
		if isRemote && h.hasRoomWatchers(m.RoomID) {
			snap, ok = h.snapshotMembersFromDB(m.RoomID)
		}
		if !ok {
			return
		}
	}

	if !isRemote {
		m.Deliver = nil
		m.Read = nil
		if m.ID == "" {
			m.ID = m.UniqId
		}
		if m.Type == MessageContentTypeSecret && h.DB != nil {
			audience := make([]string, 0, len(snap.members))
			for _, member := range snap.members {
				if member.Id != "" {
					audience = append(audience, member.Id)
				}
			}
			_ = h.DB.MessageOps().SaveSecretAudience(m.ID, audience)
		}
	}

	var firstname, lastname string
	for _, member := range snap.members {
		if member.Id == m.ClientID {
			firstname = member.FirstName
			lastname = member.LastName
			break
		}
	}

	// One shared outbound message for all local clients (writers only read).
	out := *m
	out.Deliver = nil
	out.Read = nil
	out.cachedRoomJSON = nil
	if out.Type == MessageContentTypeSecret {
		out.Content = ""
		out.SecretState = SecretLocked
	}
	needUnifiedJSON := false
	for _, cl := range snap.clients {
		if cl != nil && cl.UnifiedConn {
			needUnifiedJSON = true
			break
		}
	}
	if needUnifiedJSON {
		if raw, err := json.Marshal(roomMessageOut{
			EventType: "roomMessage",
			RoomID:    out.RoomID,
			Content:   &out,
		}); err == nil {
			out.cachedRoomJSON = raw
		}
	}
	outPtr := &out

	// Offline pup-messages: snapshot users under one RLock, send after unlock.
	var pupUsers []*User
	h.mx.RLock()
	for _, member := range snap.members {
		if member.Id == m.ClientID || snap.hasClient(member.Id) {
			continue
		}
		if user, ok := h.Users[member.Id]; ok && user != nil {
			pupUsers = append(pupUsers, user)
		}
	}
	h.mx.RUnlock()
	for _, user := range pupUsers {
		body := m.Content
		if m.Type == MessageContentTypeSecret {
			body = ""
		}
		user.trySendPupMessage(&PupMessage{
			MessageId: m.ID,
			RoomId:    m.RoomID,
			Firstname: firstname,
			Lastname:  lastname,
			Content:   body,
			Type:      m.Type,
		})
	}

	if !isRemote && h.Ntfy != nil && m.ClientID != "" {
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
		notifyBody := m.Content
		if m.Type == MessageContentTypeSecret {
			notifyBody = "پیام محرمانه"
		}
		for _, member := range snap.members {
			if member.Id == m.ClientID || snap.hasClient(member.Id) {
				continue
			}
			h.Ntfy.NotifyNewMessage(member.Id, senderName, notifyBody, m.RoomID, m.Type, "")
		}
	}

	var deliverIDs []string
	for _, cl := range snap.clients {
		if cl == nil {
			continue
		}
		if cl.ID != m.ClientID {
			deliverIDs = append(deliverIDs, cl.ID)
		}
		cl.trySendMessage(outPtr)
	}
	msgKey := m.UniqId
	if msgKey == "" {
		msgKey = m.ID
	}
	for _, id := range deliverIDs {
		h.notifyDelivered(m.ClientID, msgKey, m.RoomID, id)
	}
	if !isRemote {
		stored := *m
		stored.cachedRoomJSON = nil
		stored.Deliver = deliverIDs
		h.enqueuePersist(stored)
	}
}

func (h *Hub) Manager() {
	for {
		select {
		case req := <-h.AddConnection:
			h.mx.Lock()
			if userExists, ok := h.Users[req.UserId]; ok && userExists != nil {
				userExists.mx.Lock()
				if !userExists.closed {
					userExists.Conn[req.ConnectionId] = req.Conn
					userExists.IsConnected = true
				}
				userExists.mx.Unlock()
				if h.PresenceManager != nil {
					h.PresenceManager.AddUserConnection(req.UserId, req.ConnectionId)
				}
				go userExists.userConnection(h, req.ConnectionId)
			}
			h.mx.Unlock()
		case req := <-h.AddUnifiedConnection:
			h.mx.Lock()
			isNewUser := false
			if userExists, ok := h.Users[req.UserId]; ok && userExists != nil {
				userExists.mx.Lock()
				if !userExists.closed {
					userExists.Conn[req.ConnectionId] = req.Conn
					userExists.IsConnected = true
				}
				userExists.mx.Unlock()
			} else {
				isNewUser = true
				user := &User{
					Conn:   map[string]ConnWriter{req.ConnectionId: req.Conn},
					UserId: req.UserId,
					online: true,
				}
				newUserChannels(user)
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
			if userExists, ok := h.Users[user.UserId]; ok && userExists != nil {
				userExists.mx.Lock()
				userExists.Conn = mergeConnection(userExists.Conn, user.Conn)
				userExists.IsConnected = !userExists.closed && len(userExists.Conn) > 0
				userExists.mx.Unlock()
				connIDs := make([]string, 0, len(user.Conn))
				for s := range user.Conn {
					connIDs = append(connIDs, s)
				}
				for _, s := range connIDs {
					if h.PresenceManager != nil {
						h.PresenceManager.AddUserConnection(user.UserId, s)
					}
					go userExists.userConnection(h, s)
				}
			} else {
				newUserChannels(user)
				go user.WireRooms(h)
				h.Users[user.UserId] = user
				connIDs := make([]string, 0, len(user.Conn))
				for s := range user.Conn {
					connIDs = append(connIDs, s)
				}
				for _, s := range connIDs {
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
			var unsub []string
			var broker *RedisPubSubBroker
			u, exists := h.Users[uc.Userid]
			if exists && u != nil {
				u.mx.Lock()
				delete(u.Conn, uc.ConnectionId)
				empty := len(u.Conn) == 0
				u.mx.Unlock()
				if h.PresenceManager != nil {
					h.PresenceManager.RemoveUserConnection(uc.Userid, uc.ConnectionId)
				}
				if empty {
					unsub = h.dropUserWatchersLocked(uc.Userid)
					broker = h.RedisBroker
					now := time.Now().Unix()
					delete(h.Users, uc.Userid)
					u.shutdown()
					go func() {
						if h.DB != nil {
							h.DB.UserOps().UpdateLastSeen(uc.Userid, now)
						}
						h.updateMembersLastSeen(uc.Userid, now)
					}()
					go h.OnlineMessage(uc.Userid, offline)
				}
			}
			h.mx.Unlock()
			if broker != nil {
				broker.UnsubscribeMessages(unsub)
			}

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
		watched := h.watchersExcept(room.ID, userId)
		if len(watched) > 0 {
			for _, user := range watched {
				user.trySendRoomStatus(&RoomStatus{RoomId: room.ID, Status: status})
			}
			continue
		}
		if h.hasRoomWatchers(room.ID) {
			continue
		}
		for _, client := range room.Members {
			if client.Id == userId {
				continue
			}
			if user, ok := h.getUserLocked(client.Id); ok && user != nil {
				user.trySendRoomStatus(&RoomStatus{RoomId: room.ID, Status: status})
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

func (h *Hub) persistMessage(m Message) {
	if h.KafkaProducer != nil {
		payload := &messagebroker.MessagePayload{
			ID:        m.ID,
			Content:   m.Content,
			Type:      m.Type,
			UniqId:    m.UniqId,
			RoomID:    m.RoomID,
			Username:  m.Username,
			ClientID:  m.ClientID,
			Deliver:   m.Deliver,
			Read:      m.Read,
			CreatedAt: m.CreatedAt,
			UpdatedAt: m.UpdatedAt,
		}
		if m.ReplyTo != nil {
			b, _ := json.Marshal(m.ReplyTo)
			var ri messagebroker.ReplyInfo
			_ = json.Unmarshal(b, &ri)
			payload.ReplyTo = &ri
		}
		if m.ForwardFrom != nil {
			b, _ := json.Marshal(m.ForwardFrom)
			var fi messagebroker.ForwardInfo
			_ = json.Unmarshal(b, &fi)
			payload.ForwardFrom = &fi
		}
		if err := h.KafkaProducer.PublishMessage(payload); err != nil {
			log.Printf("kafka publish failed, falling back to direct DB write: %v", err)
			if h.DB != nil {
				h.DB.MessageOps().InsertMessage(m)
			}
		}
	} else if h.DB != nil {
		h.DB.MessageOps().InsertMessage(m)
	}

	if h.DB != nil {
		last := m
		if last.Type == MessageContentTypeSecret {
			last.Content = ""
			last.SecretState = SecretLocked
		}
		h.RoomService.UpdateLastMessage(Room{ID: m.RoomID}, last)
		if len(m.Deliver) > 0 {
			_ = h.DB.MessageOps().MessageDelivery(m.ID, m.Deliver)
		}
	}
	if h.RedisBroker != nil {
		pub := m
		if pub.Type == MessageContentTypeSecret {
			pub.Content = ""
			pub.SecretState = SecretLocked
		}
		h.RedisBroker.PublishMessage(m.RoomID, &pub)
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
		h.mx.Lock()
		if r, ok := h.Rooms[room.ID]; ok && r != nil {
			for i, m := range r.Members {
				if m.Id == userId {
					r.Members[i].LastSeen = lastSeen
					break
				}
			}
		}
		h.mx.Unlock()
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
