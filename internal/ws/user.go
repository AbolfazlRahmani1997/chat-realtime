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
	mx                 sync.Mutex
	IsConnected        bool
	Conn               map[string]ConnWriter
	StatusConnection   *websocket.Conn
	online             bool
	UserId             string `json:"UserId"`
	roomStatuses       chan *RoomStatus
	createRoom         chan *Room
	chanelNotification chan *SystemMessage
	pupMessage         chan *PupMessage
	seenMessage        chan *SeenNotification
	roomList           chan bool
}

func (User *User) WireRooms(h *Hub) {
	defer func() {

	}()
	var wg sync.WaitGroup
	for {
		select {
		case roomStatuses, ok := <-User.roomStatuses:
			if ok {
				wg.Add(1)
				go User.writeInAll(&wg)
				User.chanelNotification <- &SystemMessage{EventType: roomStatus, Content: roomStatuses}
				wg.Wait()
			}
		case roomStatuses, ok := <-User.createRoom:

			if ok {
				wg.Add(1)
				go User.writeInAll(&wg)
				User.chanelNotification <- &SystemMessage{EventType: createNewRoom, Content: roomStatuses}
				wg.Wait()
			}
		case notification, ok := <-User.pupMessage:
			{
				if ok {
					wg.Add(1)
					go User.writeInAll(&wg)
					User.chanelNotification <- &SystemMessage{EventType: incomeMessage, Content: notification}
					wg.Wait()
				}
			}
		case notification, ok := <-User.seenMessage:
			{
				if ok {
					wg.Add(1)
					go User.writeInAll(&wg)
					User.chanelNotification <- &SystemMessage{EventType: seenMessage, Content: notification}
					wg.Wait()
				}
			}

		}
	}
}

func (User *User) writeInAll(wg *sync.WaitGroup) {
	defer wg.Done()

	select {
	case sysMessage, ok := <-User.chanelNotification:
		if !ok {
			return
		}
		User.mx.Lock()
		var toRemove []string
		for s, conn := range User.Conn {
			if err := conn.WriteJSON(sysMessage); err != nil {
				toRemove = append(toRemove, s)
			}
		}
		for _, s := range toRemove {
			delete(User.Conn, s)
		}
		if len(User.Conn) == 0 {
			User.IsConnected = false
		}
		User.mx.Unlock()
	}
}

type MessageReceive struct {
	RequestType eventType `json:"event_type"`
	Item        string    `json:"item,omitempty"`
}

func (User *User) userConnection(h *Hub, connectionId string) {
	defer func() {
		User.mx.Lock()
		if c, exists := User.Conn[connectionId]; exists {
			_ = c.Close()
			delete(User.Conn, connectionId)
			h.Left <- &UserConnection{Userid: User.UserId, ConnectionId: connectionId}
		}
		User.mx.Unlock()
	}()
	User.mx.Lock()
	conn := User.Conn[connectionId]
	User.mx.Unlock()
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
				err = conn.WriteJSON(SystemMessage{EventType: listRooms, Content: h.RoomService.GetMyRoom(User.UserId, item, "")})
				if err != nil {
					fmt.Println("error reading message:", err)
					break
				}

			case seenMessage:
				h.ReadAble <- &ReadMessage{MessageId: item, UserId: User.UserId}
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
