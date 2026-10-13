package ws

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func setupBenchHub(rooms, usersPerRoom int) (*Hub, []*Client) {
	h := newTestHub()
	clients := make([]*Client, 0, rooms*usersPerRoom)
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
			cl := &Client{
				Conn:          map[string]ConnWriter{"c": nopConn{}},
				ID:            fmt.Sprintf("u-%d-%d", r, u),
				RoomID:        roomID,
				Message:       newClientMessageChan(),
				ChanelMessage: make(chan *Message, clientMessageBuffer),
				Status:        online,
				UnifiedConn:   true,
			}
			go cl.writeMessage()
			h.Register <- cl
			clients = append(clients, cl)
		}
	}
	// Let register drain.
	time.Sleep(50 * time.Millisecond)
	return h, clients
}

func BenchmarkHubBroadcast(b *testing.B) {
	h, _ := setupBenchHub(50, 20) // 50 rooms × 20 users = 1000 clients
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		roomID := fmt.Sprintf("room-%d", i%50)
		h.EnqueueBroadcast(&Message{
			UniqId:   fmt.Sprintf("m-%d", i),
			RoomID:   roomID,
			ClientID: fmt.Sprintf("u-%d-0", i%50),
			Content:  "hello load",
			Type:     "text",
		})
	}
	// Drain queue so workers finish before teardown.
	for len(h.Broadcast) > 0 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
}

func BenchmarkHubBroadcastParallel(b *testing.B) {
	h, _ := setupBenchHub(100, 10) // 1000 clients
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			i++
			roomID := fmt.Sprintf("room-%d", i%100)
			h.EnqueueBroadcast(&Message{
				UniqId:   fmt.Sprintf("p-%d-%d", i, time.Now().UnixNano()),
				RoomID:   roomID,
				ClientID: fmt.Sprintf("u-%d-0", i%100),
				Content:  "parallel",
				Type:     "text",
			})
		}
	})
	for len(h.Broadcast) > 0 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(30 * time.Millisecond)
}

func BenchmarkHubRegisterUnregister(b *testing.B) {
	h := newTestHub()
	h.EnsureRoom(Room{ID: "r1", Name: "r1", Members: []Member{{Id: "u"}}, Clients: make(map[string]*Client)})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cl := &Client{
			Conn:          map[string]ConnWriter{"c": nopConn{}},
			ID:            fmt.Sprintf("u-%d", i),
			RoomID:        "r1",
			Message:       newClientMessageChan(),
			ChanelMessage: make(chan *Message, 8),
			Status:        online,
			UnifiedConn:   true,
		}
		go cl.writeMessage()
		h.Register <- cl
		h.Unregister <- cl
	}
	time.Sleep(20 * time.Millisecond)
}

func BenchmarkEnsureRoomParallel(b *testing.B) {
	h := newTestHub()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			i++
			id := fmt.Sprintf("room-%d", i%1000)
			h.EnsureRoom(Room{ID: id, Name: id, Clients: make(map[string]*Client)})
		}
	})
}

// LoadTestHubSustained is a longer soak used with -cpuprofile/-memprofile.
func TestLoadHubSustained(t *testing.T) {
	if testing.Short() {
		t.Skip("soak load")
	}
	rooms := 100
	usersPerRoom := 20
	h, clients := setupBenchHub(rooms, usersPerRoom)

	const duration = 5 * time.Second
	var sent atomic.Int64
	var dropped atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	producers := 32
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				roomID := fmt.Sprintf("room-%d", (p+i)%rooms)
				ok := h.EnqueueBroadcast(&Message{
					UniqId:   fmt.Sprintf("load-%d-%d", p, i),
					RoomID:   roomID,
					ClientID: fmt.Sprintf("u-%d-0", (p+i)%rooms),
					Content:  "sustained-load",
					Type:     "text",
				})
				if ok {
					sent.Add(1)
				} else {
					dropped.Add(1)
				}
				i++
			}
		}(p)
	}

	time.Sleep(duration)
	close(stop)
	wg.Wait()
	for len(h.Broadcast) > 0 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)

	s := sent.Load()
	d := dropped.Load()
	t.Logf("clients=%d sent=%d dropped=%d drop_rate=%.2f%% throughput=%.0f msg/s",
		len(clients), s, d, 100*float64(d)/float64(s+d+1), float64(s)/duration.Seconds())
}
