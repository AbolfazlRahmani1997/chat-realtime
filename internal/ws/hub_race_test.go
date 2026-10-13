package ws

import (
	"sync"
	"testing"
	"time"
)

type nopConn struct{}

func (nopConn) WriteJSON(interface{}) error { return nil }
func (nopConn) WriteRawJSON([]byte) error   { return nil }
func (nopConn) Close() error                { return nil }
func (nopConn) ReadMessage() (int, []byte, error) {
	return 0, nil, nil
}

func newTestHub() *Hub {
	h := &Hub{
		Rooms:                make(map[string]*Room),
		Users:                make(map[string]*User),
		Register:             make(chan *Client, hubControlBuffer),
		ReadAble:             make(chan *ReadMessage, hubControlBuffer),
		Unregister:           make(chan *Client, hubControlBuffer),
		Broadcast:            make(chan *Message, hubBroadcastBuffer),
		Join:                 make(chan *User, hubControlBuffer),
		AddConnection:        make(chan *AddConnectionRequest, hubControlBuffer),
		AddUnifiedConnection: make(chan *AddConnectionRequest, hubControlBuffer),
		Left:                 make(chan *UserConnection, hubControlBuffer),
		Evade:                make(chan *User, hubControlBuffer),
		Room:                 make(chan *Room, hubControlBuffer),
	}
	go h.Run()
	go h.Manager()
	return h
}

func TestHubConcurrentRegisterBroadcastUnregister(t *testing.T) {
	h := newTestHub()
	const (
		rooms   = 20
		clients = 10
		msgs    = 50
	)

	for i := 0; i < rooms; i++ {
		roomID := "room-" + itoa(i)
		h.EnsureRoom(Room{
			ID:      roomID,
			Name:    roomID,
			Members: []Member{{Id: "u0"}, {Id: "u1"}},
			Clients: make(map[string]*Client),
		})
	}

	var wg sync.WaitGroup
	for i := 0; i < rooms; i++ {
		roomID := "room-" + itoa(i)
		for j := 0; j < clients; j++ {
			wg.Add(1)
			go func(roomID, userID string) {
				defer wg.Done()
				cl := &Client{
					Conn:          map[string]ConnWriter{"c1": nopConn{}},
					ID:            userID,
					RoomID:        roomID,
					Message:       newClientMessageChan(),
					ChanelMessage: make(chan *Message, clientMessageBuffer),
					Status:        online,
					UnifiedConn:   true,
				}
				go cl.writeMessage()
				go cl.readMessage(h)
				h.Register <- cl
				for k := 0; k < msgs; k++ {
					h.EnqueueBroadcast(&Message{
						UniqId:   roomID + "-" + userID + "-" + itoa(k),
						RoomID:   roomID,
						ClientID: userID,
						Content:  "x",
						Type:     "text",
					})
				}
				time.Sleep(5 * time.Millisecond)
				h.Unregister <- cl
			}(roomID, "user-"+itoa(j))
		}
	}
	wg.Wait()
	time.Sleep(50 * time.Millisecond)
}

func TestHubConcurrentUserLifecycle(t *testing.T) {
	h := newTestHub()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			uid := "user-" + itoa(i)
			h.AddUnifiedConnection <- &AddConnectionRequest{
				UserId:       uid,
				ConnectionId: "conn-1",
				Conn:         nopConn{},
			}
			time.Sleep(2 * time.Millisecond)
			if u, ok := h.getUserLocked(uid); ok {
				u.trySendRoomStatus(&RoomStatus{RoomId: "r1", Status: online})
				u.trySendPupMessage(&PupMessage{RoomId: "r1", Content: "hi"})
			}
			h.Left <- &UserConnection{Userid: uid, ConnectionId: "conn-1"}
			// After leave, sends must not panic.
			if u, ok := h.getUserLocked(uid); ok {
				u.trySendRoomStatus(&RoomStatus{RoomId: "r1", Status: offline})
			} else {
				// user already removed; create a detached user and ensure shutdown is safe
				u := &User{UserId: uid, Conn: map[string]ConnWriter{}}
				newUserChannels(u)
				u.shutdown()
				u.trySendRoomStatus(&RoomStatus{RoomId: "r1", Status: offline})
			}
		}(i)
	}
	wg.Wait()
	time.Sleep(50 * time.Millisecond)
}

func TestClientShutdownConcurrentSend(t *testing.T) {
	cl := &Client{
		Conn:          map[string]ConnWriter{"c": nopConn{}},
		ID:            "u1",
		RoomID:        "r1",
		Message:       newClientMessageChan(),
		ChanelMessage: make(chan *Message, clientMessageBuffer),
		Status:        online,
	}
	go cl.writeMessage()
	go cl.readMessage(&Hub{Broadcast: make(chan *Message, 1024)})

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cl.trySendMessage(&Message{UniqId: itoa(i), Content: "m"})
			cl.trySendChanel(&Message{UniqId: "c" + itoa(i), Content: "m"})
		}(i)
	}
	go func() {
		time.Sleep(1 * time.Millisecond)
		cl.shutdown()
	}()
	wg.Wait()
}

func itoa(i int) string {
	const digits = "0123456789"
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = digits[i%10]
		i /= 10
	}
	return string(b[pos:])
}
