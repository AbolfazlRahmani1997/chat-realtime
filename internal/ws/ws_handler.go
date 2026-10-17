package ws

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/oklog/ulid/v2"
)

var gatewayClient = &http.Client{
	Timeout: 3 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 256,
		IdleConnTimeout:     90 * time.Second,
	},
}

const gatewayUserTTL = 5 * time.Minute

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type Handler struct {
	hub         *Hub
	userMu      sync.RWMutex
	UserHandler map[string]UserRequest // cache for Gateway-validated tokens only
	MockUsers   map[string]UserRequest // mock users from file; not mixed with UserHandler
	MessageService
	mockOnly bool // when true, only mock users are accepted; no Gateway auth
}

func NewHandler(h *Hub) *Handler {
	userHandler := make(map[string]UserRequest)
	var mockUsers map[string]UserRequest
	mockOnly := false
	if loaded := LoadMockUsers(); loaded != nil {
		mockOnly = true
		mockUsers = loaded
	}
	return &Handler{
		hub:         h,
		UserHandler: userHandler,
		MockUsers:   mockUsers,
		mockOnly:    mockOnly,
	}
}

type CreateRoomReq struct {
	ID          string   `json:"Id"`
	Name        string   `json:"Name"`
	AvatarUrl   string   `json:"AvatarUrl"`
	Member      []Member `json:"Member"`
	Owner       []string `json:"Owner"`
	Writer      []string `json:"Writer"`
	RoomTypeRaw string   `json:"type,omitempty"`
	RoomType    Type     `json:"Type,omitempty"`
}

func (Handler *Handler) UpdateUser(dto UserDto) {
	Handler.hub.DB.UserOps().UpdateUser(dto.UserId, dto)
}

// Store UpdateUserPool in this Pool
func (Handler *Handler) UpdateUserPool() {
	ticker := time.NewTicker((60 * 30) * time.Second)
	for {
		select {
		case <-ticker.C:
			Handler.userMu.Lock()
			for i := range Handler.UserHandler {
				if Handler.UserHandler[i].Time.Before(time.Now()) {
					delete(Handler.UserHandler, i)
				}
			}
			Handler.userMu.Unlock()
		}
	}
}

// GetMockUsers returns list of mock users (token + label) for testing when mockOnly is true.
func (h *Handler) GetMockUsers(c *gin.Context) {
	if !h.mockOnly || h.MockUsers == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "mock users not enabled"})
		return
	}
	type item struct {
		Token string `json:"token"`
		Label string `json:"label"`
	}
	var list []item
	for tok, u := range h.MockUsers {
		label := strings.TrimSpace(u.FirstName + " " + u.LastName)
		if label == "" {
			label = u.UserName
		}
		list = append(list, item{Token: tok, Label: label})
	}
	c.JSON(http.StatusOK, gin.H{"users": list})
}

type UpdateRoomReq struct {
	Name      string   `json:"Name"`
	AvatarUrl string   `json:"AvatarUrl"`
	Members   []Member `json:"Members"`
	Type      Type     `json:"Type"`
}

func (Handler *Handler) EditRoom(c *gin.Context) {
	var req UpdateRoomReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	roomId := c.Param("roomId")
	room := Handler.hub.DB.RoomOps().GetById(roomId)
	if room.ID == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "room not found"})
		return
	}

	if req.Name != "" {
		room.Name = req.Name
	}
	if req.AvatarUrl != "" {
		room.AvatarUrl = req.AvatarUrl
	}
	if req.Type != "" {
		room.Type = req.Type
	}
	if len(req.Members) > 0 {
		room.Members = req.Members
	}

	if err := Handler.hub.DB.RoomOps().UpdateRoom(room); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var members []Member
	if len(req.Members) > 0 {
		members = req.Members
	}
	Handler.hub.PatchCachedRoom(roomId, req.Name, req.AvatarUrl, req.Type, members)

	c.JSON(http.StatusOK, room)
}

func (Handler *Handler) CreateRoom(c *gin.Context) {
	var req CreateRoomReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	roomType := normalizeRoomType(string(req.RoomType))
	if roomType == "" {
		roomType = normalizeRoomType(req.RoomTypeRaw)
	}
	if roomType == "" {
		roomType = GROUP
	}

	// اگر اتاق از قبل وجود داشته باشد، فقط آن را در Hub ثبت می‌کنیم
	existing := Handler.hub.DB.MessageOps().GetRoom(req.ID)
	if existing.ID != "" {
		Handler.hub.EnsureRoom(existing)
		c.JSON(http.StatusOK, req)
		return
	}

	// مپ کردن Owner/Writer روی Member.Roles برای ذخیره در کالکشن rooms
	ownerSet := make(map[string]bool, len(req.Owner))
	for _, id := range req.Owner {
		ownerSet[id] = true
	}
	writerSet := make(map[string]bool, len(req.Writer))
	for _, id := range req.Writer {
		writerSet[id] = true
	}

	var members []Member
	for _, m := range req.Member {
		var roles []string
		if ownerSet[m.Id] {
			roles = append(roles, "Owner")
		}
		if writerSet[m.Id] {
			roles = append(roles, "Writer")
		}
		m.Roles = roles

		Handler.hub.DB.UserOps().EnsureUser(m)
		members = append(members, m)
	}

	newRoom := Room{
		ID:      req.ID,
		Name:    req.Name,
		Type:    roomType,
		Members: members,
		Status:  offline,
		Clients: make(map[string]*Client),
	}
	if req.AvatarUrl != "" {
		newRoom.AvatarUrl = req.AvatarUrl
	}

	// ذخیره در کالکشن rooms
	Handler.hub.DB.RoomOps().Insert(newRoom)

	// اطلاع‌رسانی به تمام کاربران عضو از طریق WebSocket (رویداد createNewRoom)
	Handler.hub.Room <- &newRoom

	c.JSON(http.StatusOK, req)

}

type SetRoomAvatarReq struct {
	AvatarUrl string `json:"AvatarUrl"`
}

func (Handler *Handler) SetRoomAvatar(c *gin.Context) {
	roomId := c.Param("roomId")

	token := c.Query("token")
	if token == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "token required"})
		return
	}
	user, err := Handler.getUser(token)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}

	room := Handler.hub.DB.RoomOps().GetById(roomId)
	if room.ID == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "room not found"})
		return
	}
	if !canWrite(strconv.Itoa(user.Id), room.Members) {
		c.JSON(http.StatusForbidden, gin.H{"error": "only owners can set avatar"})
		return
	}

	var req SetRoomAvatarReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.AvatarUrl == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "AvatarUrl is required"})
		return
	}
	if err := Handler.hub.DB.RoomOps().UpdateRoomAvatar(roomId, req.AvatarUrl); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	Handler.hub.SetCachedRoomAvatar(roomId, req.AvatarUrl)
	c.JSON(http.StatusOK, gin.H{"message": "avatar updated"})
}

func (Handler *Handler) JoinRoom(c *gin.Context) {
	roomID := c.Param("roomId")
	room := Handler.hub.DB.MessageOps().GetRoom(roomID)

	token := c.Query("token")
	token = fmt.Sprintf("%s", token)
	userAuthed, errAuth := Handler.getUser(token)
	if errAuth != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": errAuth.Error()})
		return
	}
	clientID := strconv.Itoa(userAuthed.Id)
	page := c.Query("page")

	// For channel rooms: allow readers to join.
	// For other room types: keep previous behavior (only Owner/Writer can join).
	if room.Type == CHANEL {
		if !isMember(clientID, room.Members) {
			c.JSON(http.StatusForbidden, gin.H{"error": "Access Deny"})
			return
		}
	} else {
		if !canWrite(clientID, room.Members) {
			c.JSON(http.StatusForbidden, gin.H{"error": "Access Deny"})
			return
		}
	}

	canWriteVal := canWrite(clientID, room.Members)

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	go Handler.hub.RoomService.SyncUser(room)
	Handler.hub.EnsureRoom(room)
	var cl *Client
	connectionPool := make(map[string]ConnWriter)
	connectionPool[ulid.Make().String()] = &syncConn{Conn: conn}

	cl = &Client{
		Conn:          connectionPool,
		ID:            clientID,
		RoomID:        roomID,
		ChanelMessage: make(chan *Message, clientMessageBuffer),
		Message:       newClientMessageChan(),
		Status:        online,
	}
	if !canWriteVal {
		if errW := conn.WriteJSON([]string{"templates", "Salam Chetori", "ساعت ازاد کن "}); errW != nil {
			conn.Close()
			return
		}
	}

	Handler.hub.Register <- cl

	go cl.writeMessage()
	messagesRoom := Handler.hub.DB.MessageOps().RedactSecrets(Handler.hub.DB.MessageOps().GetRoomMessage(roomID, page), clientID)
	err = conn.WriteJSON(messagesRoom)
	if err != nil {
		return
	}
	messages := Handler.hub.DB.MessageOps().GetMessageNotDelivery(roomID, clientID)
	if ok := len(messages) != 0; ok {
		for i := 0; i <= len(messages)-1; i++ {
			messages[i].Deliver = append(messages[i].Deliver, clientID)
			err := Handler.hub.DB.MessageOps().MessageDelivery(messages[i].ID, messages[i].Deliver)
			if err != nil {
				return
			}
		}
	}
	cl.readMessage(Handler.hub)

}
func (Handler *Handler) GetRooms(c *gin.Context) {
	token := c.Query("token")
	token = fmt.Sprintf("%s", token)
	userAuthed, errAuth := Handler.getUser(token)
	if errAuth != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": errAuth.Error()})
		return
	}
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	userId := strconv.Itoa(userAuthed.Id)
	room := Handler.hub.RoomService.GetMyRoom(userId, "1", "")
	Handler.hub.NoteRoomWatchers(userId, roomIDsFromResponses(room))
	if len(room) == 0 {
		emptyArray := []string{}
		err := conn.WriteJSON(emptyArray)
		if err != nil {
			return
		}
	} else {
		err := conn.WriteJSON(room)
		if err != nil {
			return
		}
	}
	// اگر کاربر از قبل در Hub هست، فقط این اتصال را به همان User اضافه کن تا connectionهای قبلی (مثلاً تب دیگر) از بین نروند
	wrapped := &syncConn{Conn: conn}
	if Handler.hub.UserExists(userId) {
		connectionId := ulid.Make().String()
		Handler.hub.AddConnection <- &AddConnectionRequest{UserId: userId, ConnectionId: connectionId, Conn: wrapped}
		return
	}
	connectionId := ulid.Make().String()
	user := &User{
		Conn:   map[string]ConnWriter{connectionId: wrapped},
		UserId: userId,
		online: true,
	}
	Handler.hub.Join <- user
}

// UnifiedIncoming is the JSON shape for client-to-server events on the unified WebSocket.
type UnifiedIncoming struct {
	EventType  string     `json:"event_type"`
	Item       string     `json:"item,omitempty"`
	RoomID     string     `json:"room_id,omitempty"`
	TargetRoom string     `json:"target_room,omitempty"`
	Page       string     `json:"page,omitempty"`
	Ulid       string     `json:"ulid,omitempty"`
	Content    string     `json:"content,omitempty"`
	Type       string     `json:"type,omitempty"` // text | voice | file
	MessageID  string     `json:"message_id,omitempty"`
	ReplyTo    *ReplyInfo `json:"reply_to,omitempty"`
}

// UnifiedWS is the single WebSocket endpoint: auth, upgrade, send init (rooms), then run unified reader for events.
func (h *Handler) UnifiedWS(c *gin.Context) {

	token := c.Query("token")
	token = fmt.Sprintf("%s", token)
	userAuthed, errAuth := h.getUser(token)

	if errAuth != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": errAuth.Error()})
		return
	}
	if !h.hub.acquireConn() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "server at capacity"})
		return
	}
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		h.hub.releaseConn()
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	userId := strconv.Itoa(userAuthed.Id)
	connectionId := ulid.Make().String()

	wrapped := &syncConn{Conn: conn}
	h.hub.AddUnifiedConnection <- &AddConnectionRequest{UserId: userId, ConnectionId: connectionId, Conn: wrapped}

	rooms := h.hub.RoomService.GetMyRoom(userId, "1", "")
	h.hub.NoteRoomWatchers(userId, roomIDsFromResponses(rooms))
	if errW := wrapped.WriteJSON(map[string]interface{}{
		"event_type":      "init",
		"content":         rooms,
		"current_user_id": userId,
	}); errW != nil {
		h.hub.releaseConn()
		conn.Close()
		return
	}

	go h.unifiedReader(conn, wrapped, connectionId, userId, userAuthed.UserName)
}

func (h *Handler) unifiedReader(rawConn *websocket.Conn, writeConn ConnWriter, connectionId string, userId string, username string) {
	defer func() {
		h.hub.releaseConn()
		_ = rawConn.Close()
		for _, cl := range h.hub.CollectUnifiedClientsForConn(userId, connectionId) {
			select {
			case h.hub.Unregister <- cl:
			default:
				cl.shutdown()
			}
		}
		h.hub.Left <- &UserConnection{Userid: userId, ConnectionId: connectionId}
	}()

	var incoming UnifiedIncoming
	for {
		if err := rawConn.ReadJSON(&incoming); err != nil {

			fmt.Println(err)
			break
		}
		switch incoming.EventType {
		case "listRooms":
			page := incoming.Item
			if page == "" {
				page = "1"
			}
			rooms := h.hub.RoomService.GetMyRoom(userId, page, incoming.Content)
			h.hub.NoteRoomWatchers(userId, roomIDsFromResponses(rooms))
			_ = writeConn.WriteJSON(SystemMessage{EventType: listRooms, Content: rooms})
		case "seenMessage":
			msgId := incoming.Item
			if msgId != "" {
				h.hub.ReadAble <- &ReadMessage{MessageId: msgId, UserId: userId}
			}
		case "joinRoom":
			roomID := incoming.RoomID
			if roomID == "" {
				continue
			}
			room := h.hub.DB.MessageOps().GetRoom(roomID)
			if room.ID == "" {
				_ = writeConn.WriteJSON(map[string]interface{}{"event_type": "error", "content": "room not found"})
				continue
			}

			if room.Type == CHANEL {
				if !isMember(userId, room.Members) {
					_ = writeConn.WriteJSON(map[string]interface{}{"event_type": "error", "content": "Access Deny"})
					continue
				}
			} else if !isMember(userId, room.Members) {
				_ = writeConn.WriteJSON(map[string]interface{}{"event_type": "error", "content": "Access Deny"})
				continue
			}
			go h.hub.RoomService.SyncUser(room)
			h.hub.EnsureRoom(room)
			cl := &Client{
				Conn:          map[string]ConnWriter{connectionId: writeConn},
				ID:            userId,
				RoomID:        roomID,
				ChanelMessage: make(chan *Message, clientMessageBuffer),
				Message:       newClientMessageChan(),
				Status:        online,
				UnifiedConn:   true,
			}
			h.hub.Register <- cl
			go cl.writeMessage()
			go cl.readMessage(h.hub)

			page := incoming.Page
			if page == "" {
				page = "1"
			}
			messagesRoom := h.hub.DB.MessageOps().RedactSecrets(h.hub.DB.MessageOps().GetRoomMessage(roomID, page), userId)
			_ = writeConn.WriteJSON(map[string]interface{}{"event_type": "roomHistory", "room_id": roomID, "page": page, "content": messagesRoom})

			messages := h.hub.DB.MessageOps().GetMessageNotDelivery(roomID, userId)
			for i := range messages {
				messages[i].Deliver = append(messages[i].Deliver, userId)
				_ = h.hub.DB.MessageOps().MessageDelivery(messages[i].ID, messages[i].Deliver)
				key := messages[i].UniqId
				if key == "" {
					key = messages[i].ID
				}
				h.hub.notifyDelivered(messages[i].ClientID, key, roomID, userId)
			}
		case "leaveRoom":
			roomID := incoming.RoomID
			if roomID == "" {
				continue
			}
			if cl, ok := h.hub.GetRoomClient(roomID, userId); ok {
				h.hub.Unregister <- cl
			}
		case "sendMessage":
			roomID := incoming.RoomID
			ulidVal := incoming.Ulid
			content := incoming.Content
			if roomID == "" || ulidVal == "" {
				continue
			}
			room, ok := h.hub.GetRoomSnapshot(roomID)
			if !ok {
				continue
			}
			cl, ok := h.hub.GetRoomClient(roomID, userId)
			if !ok || cl == nil {
				continue
			}
			if ulidVal == "ping" {
				_ = writeConn.WriteJSON(map[string]interface{}{"event_type": "deliverMessage", "Content": "ping", "room_id": roomID})
				continue
			}

			// For "channel" rooms (type=chanel), only Owner/Writer can send messages.
			if room.Type == CHANEL && !canWrite(userId, room.Members) {
				_ = writeConn.WriteJSON(map[string]interface{}{"event_type": "error", "content": "forbidden", "room_id": roomID})
				continue
			}

			msgType := normalizeMessageContentType(incoming.Type)
			if msgType == MessageContentTypeFile {
				normalized, ok := normalizeFileMessage(content)
				if !ok {
					_ = writeConn.WriteJSON(map[string]interface{}{"event_type": "error", "content": "invalid file", "room_id": roomID})
					continue
				}
				content = normalized
			}
			msg := &Message{
				Content:  content,
				Type:     msgType,
				UniqId:   ulidVal,
				RoomID:   roomID,
				Username: username,
				ClientID: userId,
				ReplyTo:  incoming.ReplyTo,
			}
			if !cl.trySendChanel(msg) {
				_ = writeConn.WriteJSON(map[string]interface{}{"event_type": "error", "content": "busy", "room_id": roomID})
				continue
			}
			_ = writeConn.WriteJSON(map[string]interface{}{"event_type": "deliverMessage", "Content": ulidVal, "room_id": roomID})

			if room.Type == BOT {
				go h.forwardToBot(msg, writeConn, connectionId)
			}
		case "forwardMessage":
			messageID := incoming.MessageID
			sourceRoomID := incoming.RoomID
			targetRoomID := incoming.TargetRoom
			if messageID == "" || sourceRoomID == "" || targetRoomID == "" {
				continue
			}
			original := h.hub.DB.MessageOps().GetMessageById(messageID)
			if original.ID == "" {
				original = h.hub.DB.MessageOps().GetMessageByUniqId(messageID)
			}
			if original.ID == "" {
				_ = writeConn.WriteJSON(map[string]interface{}{"event_type": "error", "content": "message not found"})
				continue
			}
			if original.Type == MessageContentTypeSecret {
				_ = writeConn.WriteJSON(map[string]interface{}{"event_type": "error", "content": "secret messages cannot be forwarded"})
				continue
			}
			source := h.hub.DB.MessageOps().GetRoom(sourceRoomID)
			target := h.hub.DB.RoomOps().GetById(targetRoomID)
			if source.ID == "" || target.ID == "" || !isMember(userId, source.Members) || !isMember(userId, target.Members) {
				_ = writeConn.WriteJSON(map[string]interface{}{"event_type": "error", "content": "Access Deny"})
				continue
			}
			if !h.hub.RoomExists(targetRoomID) {
				h.hub.EnsureRoom(target)
			}
			fwdMsg := &Message{
				Content:  original.Content,
				Type:     original.Type,
				UniqId:   ulid.Make().String(),
				RoomID:   targetRoomID,
				Username: username,
				ClientID: userId,
				ForwardFrom: &ForwardInfo{
					FromRoomID: sourceRoomID,
					FromMsgID:  original.ID,
					Content:    original.Content,
					Username:   original.Username,
				},
			}
			h.hub.EnqueueBroadcast(fwdMsg)
			_ = writeConn.WriteJSON(map[string]interface{}{"event_type": "deliverMessage", "Content": fwdMsg.UniqId, "room_id": targetRoomID})
		case "editMessage":
			roomID := incoming.RoomID
			messageID := incoming.MessageID
			if messageID == "" {
				messageID = incoming.Ulid
			}
			content := strings.TrimSpace(incoming.Content)
			if roomID == "" || messageID == "" || content == "" || len(content) > 4000 {
				continue
			}
			msg := h.hub.DB.MessageOps().GetMessageByUniqId(messageID)
			if msg.ID == "" {
				msg = h.hub.DB.MessageOps().GetMessageById(messageID)
			}
			if msg.ID == "" || (msg.RoomID != "" && msg.RoomID != roomID) {
				_ = writeConn.WriteJSON(map[string]interface{}{"event_type": "error", "content": "message not found", "room_id": roomID})
				continue
			}
			if msg.ClientID != userId || (msg.Type != "" && msg.Type != "text") || msg.Type == MessageContentTypeSecret {
				_ = writeConn.WriteJSON(map[string]interface{}{"event_type": "error", "content": "Access Deny", "room_id": roomID})
				continue
			}
			edited, err := h.hub.DB.MessageOps().EditMessage(msg.ID, content)
			if err != nil || edited.ID == "" {
				_ = writeConn.WriteJSON(map[string]interface{}{"event_type": "error", "content": "edit failed", "room_id": roomID})
				continue
			}
			key := edited.UniqId
			if key == "" {
				key = edited.ID
			}
			h.hub.fanoutEdit(roomID, key, edited.Content)
			if h.hub.RedisBroker != nil {
				h.hub.RedisBroker.PublishRoomEvent(&PubSubRoomEvent{
					Type:      "message_edited",
					RoomID:    roomID,
					MessageID: key,
					Body:      edited.Content,
				})
			}
		case "viewSecret":
			roomID := incoming.RoomID
			messageID := incoming.MessageID
			if messageID == "" {
				messageID = incoming.Ulid
			}
			if roomID == "" || messageID == "" {
				continue
			}
			msg := h.hub.DB.MessageOps().GetMessageByUniqId(messageID)
			if msg.ID == "" {
				msg = h.hub.DB.MessageOps().GetMessageById(messageID)
			}
			if msg.ID == "" || (msg.RoomID != "" && msg.RoomID != roomID) || msg.Type != MessageContentTypeSecret {
				_ = writeConn.WriteJSON(map[string]interface{}{"event_type": "error", "content": "message not found", "room_id": roomID})
				continue
			}
			content, state, purgedNow, err := h.hub.DB.MessageOps().ViewSecret(msg.ID, userId)
			key := msg.UniqId
			if key == "" {
				key = msg.ID
			}
			if err != nil {
				_ = writeConn.WriteJSON(map[string]interface{}{"event_type": "error", "content": "Access Deny", "room_id": roomID})
				continue
			}
			switch state {
			case SecretPurged:
				h.hub.fanoutSecret(roomID, "secretPurged", key, userId)
			case SecretViewed:
				_ = writeConn.WriteJSON(map[string]interface{}{
					"event_type": "secretViewed",
					"room_id":    roomID,
					"content": map[string]string{
						"message_id": key,
						"user_id":    userId,
					},
				})
			default:
				_ = writeConn.WriteJSON(map[string]interface{}{
					"event_type": "secretBody",
					"room_id":    roomID,
					"content": map[string]string{
						"message_id": key,
						"content":    content,
					},
				})
				h.hub.fanoutSecret(roomID, "secretViewed", key, userId)
				if h.hub.RedisBroker != nil {
					h.hub.RedisBroker.PublishRoomEvent(&PubSubRoomEvent{
						Type:      "secret_viewed",
						RoomID:    roomID,
						MessageID: key,
						UserID:    userId,
					})
				}
				if purgedNow {
					h.hub.fanoutSecret(roomID, "secretPurged", key, userId)
					if h.hub.RedisBroker != nil {
						h.hub.RedisBroker.PublishRoomEvent(&PubSubRoomEvent{
							Type:      "secret_purged",
							RoomID:    roomID,
							MessageID: key,
							UserID:    userId,
						})
					}
				}
			}
		case "ping":
			_ = writeConn.WriteJSON(SystemMessage{EventType: deliverMessage, Content: "ping"})
		case "reactMessage":
			roomID := incoming.RoomID
			messageID := incoming.MessageID
			if messageID == "" {
				messageID = incoming.Ulid
			}
			emoji := strings.TrimSpace(incoming.Content)
			if roomID == "" || messageID == "" || (emoji != "" && !allowedReaction(emoji)) {
				continue
			}
			room := h.hub.DB.MessageOps().GetRoom(roomID)
			if room.ID == "" || !isMember(userId, room.Members) {
				_ = writeConn.WriteJSON(map[string]interface{}{"event_type": "error", "content": "Access Deny", "room_id": roomID})
				continue
			}
			msg := h.hub.DB.MessageOps().GetMessageByUniqId(messageID)
			if msg.ID == "" {
				msg = h.hub.DB.MessageOps().GetMessageById(messageID)
			}
			if msg.ID == "" || (msg.RoomID != "" && msg.RoomID != roomID) {
				_ = writeConn.WriteJSON(map[string]interface{}{"event_type": "error", "content": "message not found", "room_id": roomID})
				continue
			}
			reactions, err := h.hub.DB.MessageOps().ToggleReaction(msg.ID, roomID, userId, emoji)
			if err != nil {
				_ = writeConn.WriteJSON(map[string]interface{}{"event_type": "error", "content": "reaction failed", "room_id": roomID})
				continue
			}
			key := msg.UniqId
			if key == "" {
				key = msg.ID
			}
			h.hub.fanoutReaction(roomID, key, reactions)
			if h.hub.RedisBroker != nil {
				h.hub.RedisBroker.PublishRoomEvent(&PubSubRoomEvent{
					Type:      "message_reaction",
					RoomID:    roomID,
					MessageID: key,
					Reactions: reactions,
				})
			}
		case "roomHistory":
			roomID := incoming.RoomID
			page := incoming.Item
			if page == "" {
				page = "1"
			}
			if roomID == "" {
				continue
			}
			messages := h.hub.DB.MessageOps().RedactSecrets(h.hub.DB.MessageOps().GetRoomMessage(roomID, page), userId)
			_ = writeConn.WriteJSON(map[string]interface{}{
				"event_type": "roomHistory",
				"room_id":    roomID,
				"page":       page,
				"content":    messages,
			})
		default:
			// ignore unknown event_type
		}
	}
}

func (Handler *Handler) SyncRoom(c *gin.Context) {
	token := c.Query("token")
	token = fmt.Sprintf("%s", token)
	userAuthed, errAuth := Handler.getUser(token)
	if errAuth != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": errAuth.Error()})
		return
	}
	userId := strconv.Itoa(userAuthed.Id)
	if !Handler.hub.UserExists(userId) {
		c.JSON(http.StatusNotFound, gin.H{"error": "user not found in hub; connect via getRooms first"})
		return
	}
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	connectionId := ulid.Make().String()
	Handler.hub.AddConnection <- &AddConnectionRequest{UserId: userId, ConnectionId: connectionId, Conn: &syncConn{Conn: conn}}
}
func (Handler *Handler) ReadMessage(c *gin.Context) {
	roomId := c.Param("roomId")
	token := c.Query("token")
	token = fmt.Sprintf("%s", token)
	userAuthed, errAuth := Handler.getUser(token)
	if errAuth != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": errAuth.Error()})
		return
	}
	userId := strconv.Itoa(userAuthed.Id)
	client, ok := Handler.hub.GetRoomClient(roomId, userId)
	if !ok || client == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "client not found in room; join room first"})
		return
	}
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	client.setReadConn(conn)
	client.seenMessage(Handler.hub)
}

func (Handler *Handler) UpdatePin(c *gin.Context) {
	token := c.GetHeader("Authorization")
	user, err := Handler.getUser(token)
	if err != nil {
		c.JSON(403, "cant authorization")
		return
	}
	var spefic SpecificationRoom
	spefic.Pin = true
	roomId := c.Param("roomId")
	userId := strconv.Itoa(user.Id)

	newMember := Handler.hub.RoomService.updateRoomSpecification(roomId, userId, spefic)
	Handler.hub.SetCachedRoomMembers(roomId, newMember)
	for _, member := range newMember {
		if member.Id == userId {
			spefic.Pin = member.Pin
			spefic.Notification = member.Notification
		}
	}
	c.JSON(200, spefic)

}
func (Handler *Handler) UpdateNotification(c *gin.Context) {
	token := c.GetHeader("Authorization")
	user, err := Handler.getUser(token)
	if err != nil {

		c.JSON(403, "un authorization")
		return
	}
	var spefic SpecificationRoom
	spefic.Notification = true
	roomId := c.Param("roomId")
	userId := strconv.Itoa(user.Id)
	newMember := Handler.hub.RoomService.updateRoomSpecification(roomId, userId, spefic)
	Handler.hub.SetCachedRoomMembers(roomId, newMember)

	for _, member := range newMember {
		if member.Id == userId {
			spefic.Pin = member.Pin
			spefic.Notification = member.Notification
		}
	}
	c.JSON(200, spefic)
}

type ClientRes struct {
	ID       string `json:"id"`
	Username string `json:"username"`
}

func (Handler *Handler) GetClients(c *gin.Context) {
	roomId := c.Param("roomId")
	clients := Handler.hub.ListRoomClientRes(roomId)
	c.JSON(http.StatusOK, clients)
}
func hasAccess(val interface{}, array interface{}, access interface{}) (exists bool, index int, role interface{}) {
	exists = false
	index = -1
	role = nil
	switch reflect.TypeOf(array).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(array)
		for i := 0; i < s.Len(); i++ {
			arrayData := s.Index(i)

			if reflect.DeepEqual(arrayData.Field(0).String(), val.(string)) == true {
				remissions := reflect.ValueOf(arrayData.Field(1).Interface())
				for j := 0; j < remissions.Len(); j++ {
					exist, _ := InArray(arrayData.Field(1).Index(j).String(), access)
					if exist {

						role = arrayData.Field(1).Interface()
					}

				}
				index = i
				exists = true
				return

			}
		}
	}

	return
}

func InArray(needle interface{}, haystack interface{}) (exists bool, index int) {
	exists = false
	index = -1

	switch reflect.TypeOf(haystack).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(haystack)

		for i := 0; i < s.Len(); i++ {
			if reflect.DeepEqual(needle, s.Index(i).Interface()) == true {
				index = i
				exists = true
				return
			}
		}
	}

	return
}

type UserRequest struct {
	Id             int       `json:"id"`
	Avatar         string    `json:"avatar"`
	FirstName      string    `json:"firstname"`
	LastName       string    `json:"lastname"`
	UserName       string    `json:"username"`
	Time           time.Time `json:"created_at"`
	LastStatusCode int
}

// GetMe returns the current user from token (for REST /chat/me). Used by frontend to get client_id for "my message" vs "others".
func (h *Handler) GetMe(c *gin.Context) {
	token := c.Query("token")
	if token == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "token required"})
		return
	}
	user, err := h.getUser(token)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	lastSeen := h.hub.DB.UserOps().GetUserLastSeen(strconv.Itoa(user.Id))
	c.JSON(http.StatusOK, gin.H{
		"id":        user.Id,
		"firstname": user.FirstName,
		"lastname":  user.LastName,
		"username":  user.UserName,
		"avatar":    user.Avatar,
		"last_seen": lastSeen,
	})
}

func (h *Handler) GetUserLastSeen(c *gin.Context) {
	userId := c.Param("userId")
	if userId == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "userId required"})
		return
	}
	lastSeen := h.hub.DB.UserOps().GetUserLastSeen(userId)
	c.JSON(http.StatusOK, gin.H{"user_id": userId, "last_seen": lastSeen})
}

type BotRequest struct {
	UserId  string `json:"user_id"`
	Message string `json:"message"`
	RoomID  string `json:"room_id"`
	Type    string `json:"type"`
}

type BotResponse struct {
	Content string `json:"content"`
	Type    string `json:"type,omitempty"`
}

func (h *Handler) forwardToBot(msg *Message, writeConn ConnWriter, connectionId string) {
	botURL := os.Getenv("BOT_API_URL")
	if botURL == "" {
		return
	}

	body := BotRequest{
		UserId:  msg.ClientID,
		Message: msg.Content,
		RoomID:  msg.RoomID,
		Type:    msg.Type,
	}
	jsonBody, _ := json.Marshal(body)

	resp, err := http.Post(botURL, "application/json", bytes.NewReader(jsonBody))
	if err != nil {
		return
	}
	defer resp.Body.Close()

	var botResp BotResponse
	if err := json.NewDecoder(resp.Body).Decode(&botResp); err != nil {
		return
	}
	if botResp.Content == "" {
		return
	}

	botMsg := &Message{
		Content:      botResp.Content,
		Type:         botResp.Type,
		RoomID:       msg.RoomID,
		Username:     "هوش مصنوعی",
		ClientID:     "bot",
		connectionId: connectionId,
	}

	if cl, ok := h.hub.GetRoomClient(msg.RoomID, msg.ClientID); ok && cl != nil {
		botMsg.connectionId = ""
		cl.trySendMessage(botMsg)
	}
}

func (h *Handler) getUser(token string) (UserRequest, error) {
	var user UserRequest

	if h.mockOnly && h.MockUsers != nil {
		if userRequest, ok := h.MockUsers[token]; ok {
			if h.hub != nil && h.hub.DB != nil {
				h.UpdateUser(UserDto{
					UserId:    strconv.Itoa(userRequest.Id),
					UserName:  userRequest.UserName,
					FirstName: userRequest.FirstName,
					LastName:  userRequest.LastName,
					AvatarUrl: userRequest.Avatar,
				})
			}
			return userRequest, nil
		}
		return user, errors.New("unauthorized: token not in mock users")
	}
	h.userMu.RLock()
	userRequest, cached := h.UserHandler[token]
	h.userMu.RUnlock()
	if cached && userRequest.Id != 0 && userRequest.Time.After(time.Now()) {
		return userRequest, nil
	}
	gateway := fmt.Sprintf("%s/api/auth/user", os.Getenv("GATEWAY_URL"))
	request, err := http.NewRequest("GET", gateway, nil)
	if err != nil {
		return user, err
	}
	request.Header.Set("Authorization", token)
	res, err := gatewayClient.Do(request)
	if err != nil {
		return user, err
	}
	if res == nil {
		return user, errors.New("no response from gateway")
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return user, errors.New("unauthorized")
	}

	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return user, err
	}
	err = json.Unmarshal(body, &user)
	if err != nil {
		return user, err
	}
	if user.Id == 0 {
		return user, errors.New("unauthorized")
	}
	user.LastStatusCode = res.StatusCode
	user.Time = time.Now().Add(gatewayUserTTL)
	h.userMu.Lock()
	h.UserHandler[token] = user
	h.userMu.Unlock()
	h.UpdateUser(UserDto{UserId: strconv.Itoa(user.Id), UserName: user.FirstName + user.LastName, FirstName: user.FirstName, LastName: user.LastName, AvatarUrl: user.Avatar})
	return user, nil
}
