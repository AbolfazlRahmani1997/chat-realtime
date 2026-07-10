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
	Close() error
	ReadMessage() (int, []byte, error)
}

// syncConn wraps *websocket.Conn and serializes WriteJSON with a mutex.
type syncConn struct {
	Conn *websocket.Conn
	mu   sync.Mutex
}

func (c *syncConn) WriteJSON(v interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Conn.WriteJSON(v)
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

type Client struct {
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
	ReplyTo      *ReplyInfo   `json:"ReplyTo,omitempty"`
	ForwardFrom  *ForwardInfo `json:"ForwardFrom,omitempty"`
	connectionId string
	CreatedAt    time.Time `json:"CreatedAt"`
	UpdatedAt    time.Time `json:"UpdatedAt"`
}

func (c *Client) writeMessage() {
	defer func() {

	}()

	for {
		message, ok := <-c.Message
		c.writeInAll(message)
		if !ok {
			return
		}

	}
}

// writeInAll sends the message to all connections. For UnifiedConn, sends a roomMessage wrapper so frontend can route by room_id.
func (c *Client) writeInAll(m *Message) {
	if c.UnifiedConn {
		for _, conn := range c.Conn {
			err := conn.WriteJSON(map[string]interface{}{
				"event_type": "roomMessage",
				"room_id":    c.RoomID,
				"content":    m,
			})
			if err != nil {
				break
			}
		}
		return
	}
	for i, conn := range c.Conn {
		if m.connectionId != i {
			err := conn.WriteJSON(m)
			if err != nil {
				break
			}
		}
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
		err := c.Conn[index].Close()
		if err != nil {
			return
		}
		delete(c.Conn, index)

		if len(c.Conn) == 0 {
			hub.Unregister <- c
		}
	}()
	var messageDeliverClient messageClient
	for {
		_, message, err := c.Conn[index].ReadMessage()

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
			// For "channel" rooms (type=chanel), only Owner/Writer can publish.
			if room, ok := hub.Rooms[c.RoomID]; ok && room.Type == CHANEL && !canWrite(c.ID, room.Members) {
				_ = c.Conn[index].WriteJSON(map[string]interface{}{
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
			c.ChanelMessage <- msg
			systemMessage := SystemMessage{EventType: deliverMessage, Content: messageDeliverClient.Ulid}
			err = c.Conn[index].WriteJSON(systemMessage)

		} else {
			systemMessage := SystemMessage{EventType: deliverMessage, Content: messageDeliverClient.Ulid}
			err = c.Conn[index].WriteJSON(systemMessage)
		}
		if err != nil {

			break
		}

	}
}

func (c *Client) readMessage(hub *Hub) {
	defer func() {
		hub.Unregister <- c
	}()

	for {
		m, ok := <-c.ChanelMessage
		if !ok {
			break
		}
		hub.Broadcast <- m
	}
}

func (c *Client) seenMessage(hub *Hub) {
	defer func() {
		c.ReadMessage.Close()
	}()

	for {
		_, m, err := c.ReadMessage.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v", err)
			}
			break
		}
		msg := &ReadMessage{
			MessageId: string(m),
			UserId:    c.ID,
		}

		hub.ReadAble <- msg
	}
}
