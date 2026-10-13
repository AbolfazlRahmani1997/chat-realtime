package ws

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"time"
)

// LoadConnFactory builds a ConnWriter for a seeded load-test client.
type LoadConnFactory func(roomID, userID string) ConnWriter

// nopLoadConn is a no-op ConnWriter for in-process load tests.
type nopLoadConn struct{}

func (nopLoadConn) WriteJSON(interface{}) error       { return nil }
func (nopLoadConn) WriteRawJSON([]byte) error         { return nil }
func (nopLoadConn) Close() error                      { return nil }
func (nopLoadConn) ReadMessage() (int, []byte, error) { return 0, nil, nil }

// NewLoadHub creates a Hub with Run/Manager started and no external deps.
func NewLoadHub() *Hub {
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

// SeedLoadHub registers rooms and unified clients. Returns total client count.
func SeedLoadHub(h *Hub, rooms, usersPerRoom int) int {
	return SeedLoadHubWithConn(h, rooms, usersPerRoom, func(string, string) ConnWriter {
		return nopLoadConn{}
	})
}

// SeedLoadHubWithConn is like SeedLoadHub but lets the caller inject ConnWriters
// (e.g. latency-recording conns for capacity tests).
func SeedLoadHubWithConn(h *Hub, rooms, usersPerRoom int, factory LoadConnFactory) int {
	if factory == nil {
		factory = func(string, string) ConnWriter { return nopLoadConn{} }
	}
	total := 0
	for r := 0; r < rooms; r++ {
		roomID := fmt.Sprintf("room-%d", r)
		members := make([]Member, 0, usersPerRoom)
		for u := 0; u < usersPerRoom; u++ {
			members = append(members, Member{Id: fmt.Sprintf("u-%d-%d", r, u)})
		}
		h.EnsureRoom(Room{
			ID:      roomID,
			Name:    roomID,
			Type:    GROUP,
			Members: members,
			Clients: make(map[string]*Client),
		})
		for u := 0; u < usersPerRoom; u++ {
			userID := fmt.Sprintf("u-%d-%d", r, u)
			cl := &Client{
				Conn:          map[string]ConnWriter{"c": factory(roomID, userID)},
				ID:            userID,
				RoomID:        roomID,
				Message:       newClientMessageChan(),
				ChanelMessage: make(chan *Message, clientMessageBuffer),
				Status:        online,
				UnifiedConn:   true,
			}
			go cl.writeMessage()
			h.Register <- cl
			total++
		}
	}
	time.Sleep(150 * time.Millisecond)
	return total
}

// WaitBroadcastDrain waits until the broadcast queue is empty or timeout.
func WaitBroadcastDrain(h *Hub, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(h.Broadcast) == 0 {
			time.Sleep(20 * time.Millisecond)
			if len(h.Broadcast) == 0 {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
}

// DroppedBroadcasts returns how many messages EnqueueBroadcast discarded.
func (h *Hub) DroppedBroadcasts() uint64 {
	return h.droppedBroadcasts.Load()
}

// ResetDroppedBroadcasts clears the drop counter (for stepped capacity tests).
func (h *Hub) ResetDroppedBroadcasts() {
	h.droppedBroadcasts.Store(0)
}

// LatencyConn records end-to-end delivery latency using Message.CreatedAt.
type LatencyConn struct {
	Delivered *atomic.Int64
	// LatenciesNs stores up to Cap samples (lock-free best-effort via index).
	LatenciesNs []int64
	idx         atomic.Uint64
	Cap         uint64
}

func NewLatencyConn(delivered *atomic.Int64, buf []int64) *LatencyConn {
	return &LatencyConn{
		Delivered:   delivered,
		LatenciesNs: buf,
		Cap:         uint64(len(buf)),
	}
}

func (c *LatencyConn) recordCreatedAt(createdAt time.Time) {
	if createdAt.IsZero() {
		return
	}
	lat := time.Since(createdAt).Nanoseconds()
	if lat < 0 {
		lat = 0
	}
	if c.Cap > 0 {
		i := c.idx.Add(1) - 1
		if i < c.Cap {
			c.LatenciesNs[i] = lat
		}
	}
}

func (c *LatencyConn) WriteJSON(v interface{}) error {
	c.Delivered.Add(1)
	switch msg := v.(type) {
	case roomMessageOut:
		if msg.Content != nil {
			c.recordCreatedAt(msg.Content.CreatedAt)
		}
	case *roomMessageOut:
		if msg != nil && msg.Content != nil {
			c.recordCreatedAt(msg.Content.CreatedAt)
		}
	case map[string]interface{}:
		if content, ok := msg["content"].(*Message); ok && content != nil {
			c.recordCreatedAt(content.CreatedAt)
		}
	}
	return nil
}

func (c *LatencyConn) WriteRawJSON(data []byte) error {
	c.Delivered.Add(1)
	c.recordCreatedAt(createdAtFromRoomJSON(data))
	return nil
}

func createdAtFromRoomJSON(data []byte) time.Time {
	const key = `"CreatedAt":"`
	i := bytes.Index(data, []byte(key))
	if i < 0 {
		return time.Time{}
	}
	rest := data[i+len(key):]
	j := bytes.IndexByte(rest, '"')
	if j <= 0 {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, string(rest[:j]))
	if err != nil {
		t, err = time.Parse(time.RFC3339, string(rest[:j]))
		if err != nil {
			return time.Time{}
		}
	}
	return t
}

func (c *LatencyConn) Close() error                      { return nil }
func (c *LatencyConn) ReadMessage() (int, []byte, error) { return 0, nil, nil }

func (c *LatencyConn) SampleCount() int {
	n := c.idx.Load()
	if n > c.Cap {
		return int(c.Cap)
	}
	return int(n)
}
