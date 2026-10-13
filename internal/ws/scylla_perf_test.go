package ws_test

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ScyllaRepo "server/adapters/Reporisotires/ScyllaDB"
	"server/internal/ws"
)

func scyllaProviderOrSkip(t testing.TB) *ws.ScyllaProvider {
	t.Helper()
	if os.Getenv("SCYLLA_PERF") == "0" {
		t.Skip("SCYLLA_PERF=0")
	}
	if os.Getenv("SCYLLA_KEYSPACE") == "" {
		_ = os.Setenv("SCYLLA_KEYSPACE", "chat_ks_perf")
	}
	session, err := ScyllaRepo.OpenSession()
	if err != nil {
		t.Skipf("scylla unavailable: %v", err)
	}
	t.Cleanup(session.Close)
	return ws.NewScyllaProvider(session)
}

// TestScyllaMessageWriteReadPerf measures insert + history read latency/throughput.
//
//	SCYLLA_HOSTS=localhost go test ./internal/ws -run TestScyllaMessageWriteReadPerf -count=1 -v
func TestScyllaMessageWriteReadPerf(t *testing.T) {
	p := scyllaProviderOrSkip(t)

	roomID := fmt.Sprintf("perf-room-%d", time.Now().UnixNano())
	userA, userB := "user-a", "user-b"
	p.InsertRoom(ws.Room{
		ID: roomID, Name: "perf", Type: ws.GROUP,
		Members: []ws.Member{{Id: userA}, {Id: userB, Pin: true}},
	})

	const writers = 32
	const perWriter = 200
	total := writers * perWriter

	var (
		okInsert atomic.Int64
		failIns  atomic.Int64
		latSum   atomic.Int64
	)

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				id := fmt.Sprintf("%s-%d-%d", roomID, worker, i)
				t0 := time.Now()
				p.InsertMessageFlat(id, "hello", "text", id, roomID, "u", userA, nil, nil, nil, nil, time.Now(), time.Now())
				latSum.Add(time.Since(t0).Nanoseconds())
				// cheap verify: PK read
				if m := p.GetMessage(id); m.ID == "" {
					failIns.Add(1)
				} else {
					okInsert.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
	writeDur := time.Since(start)

	readStart := time.Now()
	msgs := p.GetAllMessages(roomID, userB)
	readDur := time.Since(readStart)

	roomsStart := time.Now()
	rooms := p.GetMyRooms(userB, 1, 20, "")
	roomsDur := time.Since(roomsStart)

	delStart := time.Now()
	if err := p.MessageDelivery(fmt.Sprintf("%s-0-0", roomID), []string{userB}); err != nil {
		t.Fatalf("MessageDelivery: %v", err)
	}
	delDur := time.Since(delStart)

	avgWrite := time.Duration(latSum.Load() / int64(total))
	writeRPS := float64(total) / writeDur.Seconds()

	t.Logf("writes: total=%d ok=%d fail=%d wall=%s avg=%s rps=%.0f",
		total, okInsert.Load(), failIns.Load(), writeDur, avgWrite, writeRPS)
	t.Logf("history(GetAllMessages): n=%d dur=%s", len(msgs), readDur)
	t.Logf("GetMyRooms: n=%d dur=%s", len(rooms), roomsDur)
	t.Logf("MessageDelivery: dur=%s", delDur)

	if failIns.Load() > 0 {
		t.Fatalf("insert verify failures: %d", failIns.Load())
	}
	if len(msgs) == 0 {
		t.Fatal("expected history messages")
	}
	if len(rooms) == 0 {
		t.Fatal("expected rooms_by_user to return room for userB")
	}
	if writeRPS < 200 {
		t.Fatalf("write throughput too low: %.0f rps (want >= 200)", writeRPS)
	}
	if avgWrite > 50*time.Millisecond {
		t.Fatalf("avg write latency too high: %s (want <= 50ms)", avgWrite)
	}
	if readDur > 100*time.Millisecond {
		t.Fatalf("history read too slow: %s (want <= 100ms)", readDur)
	}
	if roomsDur > 50*time.Millisecond {
		t.Fatalf("GetMyRooms too slow: %s (want <= 50ms)", roomsDur)
	}
}

// BenchmarkScyllaInsertMessageFlat runs against live Scylla when available.
//
//	SCYLLA_HOSTS=localhost go test ./internal/ws -bench BenchmarkScyllaInsertMessageFlat -benchtime=3s -count=1
func BenchmarkScyllaInsertMessageFlat(b *testing.B) {
	p := scyllaProviderOrSkip(b)
	roomID := fmt.Sprintf("bench-room-%d", time.Now().UnixNano())
	p.InsertRoom(ws.Room{
		ID: roomID, Name: "bench", Type: ws.GROUP,
		Members: []ws.Member{{Id: "bench-user"}},
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("%s-%d", roomID, i)
		p.InsertMessageFlat(id, "x", "text", id, roomID, "u", "bench-user", nil, nil, nil, nil, time.Now(), time.Now())
	}
}
