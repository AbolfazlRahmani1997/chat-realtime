package ws

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ConnWriter is used so the same connection can be written from User (notifications) and Client (roomMessage) without concurrent write panic.
type ConnWriter interface {
	WriteJSON(interface{}) error
	// WriteRawJSON writes a pre-marshaled JSON text frame (avoids per-client marshal).
	WriteRawJSON([]byte) error
	Close() error
	ReadMessage() (int, []byte, error)
}

// roomMessageOut is the UnifiedConn WS envelope. Typed (not map) to cut allocs on the hot path.
type roomMessageOut struct {
	EventType string   `json:"event_type"`
	RoomID    string   `json:"room_id"`
	Content   *Message `json:"content"`
}

// syncConn wraps *websocket.Conn and serializes writes with a mutex.
type syncConn struct {
	Conn *websocket.Conn
	mu   sync.Mutex
}

func (c *syncConn) WriteJSON(v interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Conn.WriteJSON(v)
}

func (c *syncConn) WriteRawJSON(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Conn.WriteMessage(websocket.TextMessage, data)
}

func (c *syncConn) Close() error {
	return c.Conn.Close()
}

func (c *syncConn) ReadMessage() (int, []byte, error) {
	return c.Conn.ReadMessage()
}

type status string

const (
	online  status = "online"
	offline status = "offline"
	evade   status = "evade"
)

const clientMessageBuffer = 64

type Client struct {
	mu            sync.Mutex
	closed        bool
	Conn          map[string]ConnWriter
	ReadMessage   *websocket.Conn
	Message       chan *Message
	ChanelMessage chan *Message
	ID            string `json:"id"`
	RoomID        string `json:"roomId"`
	Username      string `json:"username"`
	Status        status `json:"status"`
	// UnifiedConn: when true, this Client shares one WS with User; reader is in UnifiedWS handler and writeInAll sends roomMessage wrapper.
	UnifiedConn bool
}

type ReplyInfo struct {
	ID       string `json:"id"`
	Content  string `json:"content"`
	Username string `json:"username"`
}

type ForwardInfo struct {
	FromRoomID string `json:"from_room_id"`
	FromMsgID  string `json:"from_msg_id"`
	Content    string `json:"content"`
	Username   string `json:"username"`
}

// Reaction is one emoji grouped across the users who set it on a message.
type Reaction struct {
	Emoji   string   `json:"emoji"`
	Count   int      `json:"count"`
	UserIDs []string `json:"user_ids"`
}

type Message struct {
	ID           string       `json:"_id"`
	Content      string       `json:"Content,omitempty"`
	Type         string       `json:"Type,omitempty"`
	UniqId       string       `json:"UniqId"`
	RoomID       string       `json:"RoomID,omitempty"`
	Username     string       `json:"Username,omitempty"`
	ClientID     string       `json:"ClientID,omitempty"`
	Deliver      []string     `json:"Deliver"`
	Read         []string     `json:"Read"`
	Reactions    []Reaction   `json:"Reactions,omitempty"`
	ReplyTo      *ReplyInfo   `json:"ReplyTo,omitempty"`
	ForwardFrom  *ForwardInfo `json:"ForwardFrom,omitempty"`
	Edited       bool         `json:"Edited,omitempty"`
	SecretState  string       `json:"SecretState,omitempty"`
	connectionId string
	remote       bool // set for cross-instance Redis fan-out; skip persist/re-publish
	// cachedRoomJSON: pre-marshaled roomMessageOut for UnifiedConn fan-out (json omitted).
	cachedRoomJSON []byte    `json:"-"`
	CreatedAt      time.Time `json:"CreatedAt"`
	UpdatedAt      time.Time `json:"UpdatedAt"`
}

// newClientMessageChan returns a buffered outbound message channel.
func newClientMessageChan() chan *Message {
	return make(chan *Message, clientMessageBuffer)
}

// shutdown closes outbound/inbound channels exactly once so writers cannot panic.
func (c *Client) shutdown() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	c.Status = offline
	if c.Message != nil {
		close(c.Message)
	}
	if c.ChanelMessage != nil {
		close(c.ChanelMessage)
	}
}

func (c *Client) setStatus(s status) {
	if c == nil {
		return
	}
	c.mu.Lock()
	if !c.closed {
		c.Status = s
	}
	c.mu.Unlock()
}

func (c *Client) setReadConn(conn *websocket.Conn) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.ReadMessage = conn
	c.mu.Unlock()
}

func (c *Client) takeReadConn() *websocket.Conn {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ReadMessage
}

// trySendMessage non-blockingly delivers m to the client's write pump.
func (c *Client) trySendMessage(m *Message) bool {
	if c == nil || m == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.Message == nil {
		return false
	}
	select {
	case c.Message <- m:
		return true
	default:
		return false
	}
}

// trySendChanel non-blockingly queues an inbound message for readMessage → Broadcast.
func (c *Client) trySendChanel(m *Message) bool {
	if c == nil || m == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.ChanelMessage == nil {
		return false
	}
	select {
	case c.ChanelMessage <- m:
		return true
	default:
		return false
	}
}

func (c *Client) writeJSONAll(v interface{}) {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	conns := make([]ConnWriter, 0, len(c.Conn))
	for _, conn := range c.Conn {
		if conn != nil {
			conns = append(conns, conn)
		}
	}
	c.mu.Unlock()
	for _, conn := range conns {
		_ = conn.WriteJSON(v)
	}
}

func (c *Client) writeMessage() {
	for {
		message, ok := <-c.Message
		if !ok {
			return
		}
		c.writeInAll(message)
	}
}

// writeInAll sends the message to all connections. For UnifiedConn, sends a roomMessage wrapper so frontend can route by room_id.
func (c *Client) writeInAll(m *Message) {
	if m == nil {
		return
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	unified := c.UnifiedConn
	roomID := c.RoomID
	connID := m.connectionId

	// Fast path: 0–2 conns (typical UnifiedConn) — no slice alloc.
	var c0, c1 ConnWriter
	var id0, id1 string
	n := 0
	for id, conn := range c.Conn {
		if conn == nil {
			continue
		}
		switch n {
		case 0:
			c0, id0 = conn, id
			n = 1
		case 1:
			c1, id1 = conn, id
			n = 2
		default:
			n = 3
		}
		if n > 2 {
			break
		}
	}
	if n > 2 {
		// Rare multi-tab path: collect then write outside the lock.
		conns := make([]ConnWriter, 0, len(c.Conn))
		ids := make([]string, 0, len(c.Conn))
		for id, conn := range c.Conn {
			if conn != nil {
				conns = append(conns, conn)
				ids = append(ids, id)
			}
		}
		c.mu.Unlock()
		for i := range conns {
			c.writeOne(m, unified, roomID, connID, ids[i], conns[i])
		}
		return
	}
	c.mu.Unlock()

	switch n {
	case 0:
		return
	case 1:
		c.writeOne(m, unified, roomID, connID, id0, c0)
	case 2:
		c.writeOne(m, unified, roomID, connID, id0, c0)
		c.writeOne(m, unified, roomID, connID, id1, c1)
	}
}

func (c *Client) writeOne(m *Message, unified bool, roomID, msgConnID, connID string, conn ConnWriter) {
	if conn == nil {
		return
	}
	if unified {
		if len(m.cachedRoomJSON) > 0 {
			_ = conn.WriteRawJSON(m.cachedRoomJSON)
			return
		}
		_ = conn.WriteJSON(roomMessageOut{
			EventType: "roomMessage",
			RoomID:    roomID,
			Content:   m,
		})
		return
	}
	if msgConnID != connID {
		_ = conn.WriteJSON(m)
	}
}

type messageClient struct {
	Ulid    string     `json:"ulid"`
	Content string     `json:"content"`
	Type    string     `json:"type,omitempty"`
	ReplyTo *ReplyInfo `json:"reply_to,omitempty"`
}

func (c *Client) readerMessage(index string, hub *Hub) {
	defer func() {
		c.mu.Lock()
		conn, exists := c.Conn[index]
		if exists {
			delete(c.Conn, index)
		}
		empty := len(c.Conn) == 0
		c.mu.Unlock()
		if exists && conn != nil {
			_ = conn.Close()
		}
		if empty {
			select {
			case hub.Unregister <- c:
			default:
				// Hub busy; shutdown locally so writers stop.
				c.shutdown()
			}
		}
	}()

	c.mu.Lock()
	conn := c.Conn[index]
	c.mu.Unlock()
	if conn == nil {
		return
	}

	var messageDeliverClient messageClient
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v", err)
			}
			break
		}

		err = json.Unmarshal(message, &messageDeliverClient)
		if err != nil {
			return
		}
		if messageDeliverClient.Ulid != "ping" {
			snap, ok := hub.GetRoomSnapshot(c.RoomID)
			if ok && snap.Type == CHANEL && !canWrite(c.ID, snap.Members) {
				_ = conn.WriteJSON(map[string]interface{}{
					"event_type": "error",
					"content":    "forbidden",
					"room_id":    c.RoomID,
				})
				continue
			}

			msg := &Message{
				Content:      messageDeliverClient.Content,
				Type:         normalizeMessageContentType(messageDeliverClient.Type),
				UniqId:       messageDeliverClient.Ulid,
				connectionId: index,
				RoomID:       c.RoomID,
				Username:     c.Username,
				ClientID:     c.ID,
				ReplyTo:      messageDeliverClient.ReplyTo,
			}
			if !c.trySendChanel(msg) {
				continue
			}
			systemMessage := SystemMessage{EventType: deliverMessage, Content: messageDeliverClient.Ulid}
			err = conn.WriteJSON(systemMessage)
		} else {
			systemMessage := SystemMessage{EventType: deliverMessage, Content: messageDeliverClient.Ulid}
			err = conn.WriteJSON(systemMessage)
		}
		if err != nil {
			break
		}
	}
}

func (c *Client) readMessage(hub *Hub) {
	for {
		m, ok := <-c.ChanelMessage
		if !ok {
			return
		}
		hub.EnqueueBroadcast(m)
	}
}

func (c *Client) seenMessage(hub *Hub) {
	conn := c.takeReadConn()
	if conn == nil {
		return
	}
	defer func() {
		_ = conn.Close()
		c.setReadConn(nil)
	}()

	for {
		_, m, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v", err)
			}
			break
		}
		select {
		case hub.ReadAble <- &ReadMessage{MessageId: string(m), UserId: c.ID}:
		default:
		}
	}
}
