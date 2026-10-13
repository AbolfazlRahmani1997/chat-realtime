package ws

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	"github.com/gorilla/websocket"
)

type eventType string

type messagePop struct {
	roomId string
	body   string
}

const (
	roomStatus     eventType = "roomStatus"
	incomeMessage  eventType = "incomeMessage"
	listRooms      eventType = "listRooms"
	seenMessage    eventType = "seenMessage"
	deliverMessage eventType = "deliverMessage"
	createNewRoom  eventType = "createNewRoom"
)

type SystemMessage struct {
	EventType eventType `json:"event_type"`
	Content   interface{}
}

type PupMessage struct {
	MessageId string `json:"message_id,omitempty"`
	RoomId    string `json:"room_id,omitempty"`
	Lastname  string `json:"lastname,omitempty"`
	Firstname string `json:"firstname,omitempty"`
	Content   string `json:"content"`
	Type      string `json:"type,omitempty"`
}

type SeenNotification struct {
	MessageId string `json:"message_id,omitempty"`
	RoomId    string `json:"room_id,omitempty"`
}

type UserConnection struct {
	Userid       string
	ConnectionId string
}

type User struct {
	mx           sync.Mutex
	closed       bool
	IsConnected  bool
	Conn         map[string]ConnWriter
	StatusConnection *websocket.Conn
	online       bool
	UserId       string `json:"UserId"`
	roomStatuses chan *RoomStatus
	createRoom   chan *Room
	pupMessage   chan *PupMessage
	seenMessage  chan *SeenNotification
}

func newUserChannels(u *User) {
	u.roomStatuses = make(chan *RoomStatus, 32)
	u.pupMessage = make(chan *PupMessage, 32)
	u.createRoom = make(chan *Room, 16)
	u.seenMessage = make(chan *SeenNotification, 32)
	u.IsConnected = true
}

// shutdown closes notification channels exactly once so WireRooms can exit
// and in-flight senders do not panic on a closed channel.
// Channel fields are left non-nil so WireRooms can observe the close.
func (u *User) shutdown() {
	if u == nil {
		return
	}
	u.mx.Lock()
	defer u.mx.Unlock()
	if u.closed {
		return
	}
	u.closed = true
	u.IsConnected = false
	close(u.roomStatuses)
	close(u.createRoom)
	close(u.pupMessage)
	close(u.seenMessage)
}

func (u *User) trySendRoomStatus(st *RoomStatus) {
	if u == nil || st == nil {
		return
	}
	u.mx.Lock()
	defer u.mx.Unlock()
	if u.closed {
		return
	}
	select {
	case u.roomStatuses <- st:
	default:
	}
}

func (u *User) trySendCreateRoom(room *Room) {
	if u == nil || room == nil {
		return
	}
	u.mx.Lock()
	defer u.mx.Unlock()
	if u.closed {
		return
	}
	select {
	case u.createRoom <- room:
	default:
	}
}

func (u *User) trySendPupMessage(msg *PupMessage) {
	if u == nil || msg == nil {
		return
	}
	u.mx.Lock()
	defer u.mx.Unlock()
	if u.closed {
		return
	}
	select {
	case u.pupMessage <- msg:
	default:
	}
}

func (u *User) trySendSeen(n *SeenNotification) {
	if u == nil || n == nil {
		return
	}
	u.mx.Lock()
	defer u.mx.Unlock()
	if u.closed {
		return
	}
	select {
	case u.seenMessage <- n:
	default:
	}
}

func (u *User) hasConnections() bool {
	if u == nil {
		return false
	}
	u.mx.Lock()
	defer u.mx.Unlock()
	return !u.closed && len(u.Conn) > 0
}

func (u *User) WireRooms(h *Hub) {
	for {
		select {
		case st, ok := <-u.roomStatuses:
			if !ok {
				return
			}
			u.dispatch(&SystemMessage{EventType: roomStatus, Content: st})
		case room, ok := <-u.createRoom:
			if !ok {
				return
			}
			u.dispatch(&SystemMessage{EventType: createNewRoom, Content: room})
		case notification, ok := <-u.pupMessage:
			if !ok {
				return
			}
			u.dispatch(&SystemMessage{EventType: incomeMessage, Content: notification})
		case notification, ok := <-u.seenMessage:
			if !ok {
				return
			}
			u.dispatch(&SystemMessage{EventType: seenMessage, Content: notification})
		}
	}
}

func (u *User) dispatch(sysMessage *SystemMessage) {
	if sysMessage == nil {
		return
	}
	u.mx.Lock()
	defer u.mx.Unlock()
	if u.closed {
		return
	}
	var toRemove []string
	for s, conn := range u.Conn {
		if conn == nil {
			toRemove = append(toRemove, s)
			continue
		}
		if err := conn.WriteJSON(sysMessage); err != nil {
			toRemove = append(toRemove, s)
		}
	}
	for _, s := range toRemove {
		delete(u.Conn, s)
	}
	if len(u.Conn) == 0 {
		u.IsConnected = false
	}
}

type MessageReceive struct {
	RequestType eventType `json:"event_type"`
	Item        string    `json:"item,omitempty"`
}

func (u *User) userConnection(h *Hub, connectionId string) {
	defer func() {
		u.mx.Lock()
		c, exists := u.Conn[connectionId]
		if exists {
			delete(u.Conn, connectionId)
		}
		u.mx.Unlock()
		if exists && c != nil {
			_ = c.Close()
		}
		if exists {
			// Never hold User.mx while sending on Hub channels (Manager may need User.mx).
			select {
			case h.Left <- &UserConnection{Userid: u.UserId, ConnectionId: connectionId}:
			default:
				// Manager backlog; presence cleanup may lag until next reconnect.
			}
		}
	}()
	u.mx.Lock()
	conn := u.Conn[connectionId]
	u.mx.Unlock()
	if conn == nil {
		return
	}
	var messageClient MessageReceive
	var item string
	var eventRequest eventType
	for {
		if eventRequest != "" {
			switch eventRequest {
			case listRooms:
				_, err := strconv.Atoi(item)
				if err != nil {
					break
				}
				err = conn.WriteJSON(SystemMessage{EventType: listRooms, Content: h.RoomService.GetMyRoom(u.UserId, item, "")})
				if err != nil {
					fmt.Println("error reading message:", err)
					break
				}

			case seenMessage:
				select {
				case h.ReadAble <- &ReadMessage{MessageId: item, UserId: u.UserId}:
				default:
				}
			}

		}

		var message []byte
		_, message, err := conn.ReadMessage()
		if err != nil {
			fmt.Println(err)
			break
		}
		if len(message) > 0 {
			err = json.Unmarshal(message, &messageClient)
			if err != nil {
				break
			}
			eventRequest = messageClient.RequestType
			item = messageClient.Item
		} else {
			item = ""
		}
	}
}
