// Scylla write/read capacity smoke test for chat-realtime.
//
//	SCYLLA_HOSTS=localhost go run ./cmd/scyllabench -writers=32 -n=200
package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	ScyllaRepo "server/adapters/Reporisotires/ScyllaDB"
	"server/internal/ws"
)

func main() {
	writers := flag.Int("writers", 32, "concurrent writers")
	perWriter := flag.Int("n", 200, "messages per writer")
	flag.Parse()

	if os.Getenv("SCYLLA_KEYSPACE") == "" {
		_ = os.Setenv("SCYLLA_KEYSPACE", "chat_ks_perf")
	}

	session, err := ScyllaRepo.OpenSession()
	if err != nil {
		fmt.Fprintf(os.Stderr, "scylla connect failed: %v\n", err)
		os.Exit(1)
	}
	defer session.Close()
	p := ws.NewScyllaProvider(session)

	roomID := fmt.Sprintf("bench-%d", time.Now().UnixNano())
	userA, userB := "bench-a", "bench-b"
	p.InsertRoom(ws.Room{
		ID: roomID, Name: "scyllabench", Type: ws.GROUP,
		Members: []ws.Member{{Id: userA}, {Id: userB}},
	})

	total := *writers * *perWriter
	var ok, fail, latSum atomic.Int64

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(*writers)
	for w := 0; w < *writers; w++ {
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < *perWriter; i++ {
				id := fmt.Sprintf("%s-%d-%d", roomID, worker, i)
				t0 := time.Now()
				p.InsertMessageFlat(id, "hello", "text", id, roomID, "u", userA, nil, nil, nil, nil, time.Now(), time.Now())
				latSum.Add(time.Since(t0).Nanoseconds())
				if m := p.GetMessage(id); m.ID == "" {
					fail.Add(1)
				} else {
					ok.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
	wall := time.Since(start)

	histStart := time.Now()
	msgs := p.GetAllMessages(roomID, userB)
	histDur := time.Since(histStart)

	roomsStart := time.Now()
	rooms := p.GetMyRooms(userB, 1, 20, "")
	roomsDur := time.Since(roomsStart)

	avg := time.Duration(0)
	if total > 0 {
		avg = time.Duration(latSum.Load() / int64(total))
	}
	rps := float64(total) / wall.Seconds()

	fmt.Printf("writes total=%d ok=%d fail=%d wall=%s avg=%s rps=%.0f\n", total, ok.Load(), fail.Load(), wall, avg, rps)
	fmt.Printf("history n=%d dur=%s\n", len(msgs), histDur)
	fmt.Printf("GetMyRooms n=%d dur=%s\n", len(rooms), roomsDur)

	if fail.Load() > 0 || len(msgs) == 0 || len(rooms) == 0 {
		os.Exit(2)
	}
}
