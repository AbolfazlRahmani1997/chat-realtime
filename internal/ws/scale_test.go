package ws

import "testing"

func TestMessageShardStable(t *testing.T) {
	t.Setenv("REDIS_MESSAGE_SHARDS", "64")
	a := messageShard("room-1")
	if a != messageShard("room-1") {
		t.Fatal("shard changed for the same room")
	}
	if a < 0 || a >= 64 {
		t.Fatalf("shard %d out of range", a)
	}
	if messageChannel("room-1") == "" {
		t.Fatal("empty channel")
	}
}

func TestAcquireConnHonorsCap(t *testing.T) {
	t.Setenv("MAX_WS_CONNECTIONS", "2")
	h := &Hub{}
	if !h.acquireConn() || !h.acquireConn() {
		t.Fatal("expected two live connections")
	}
	if h.acquireConn() {
		t.Fatal("cap was not enforced")
	}
	h.releaseConn()
	if !h.acquireConn() {
		t.Fatal("expected a slot after release")
	}
}

func TestRoomWatchersSubscribeAndDrop(t *testing.T) {
	h := &Hub{}
	h.NoteRoomWatchers("u1", []string{"r1", "r1", ""})
	if !h.hasRoomWatchers("r1") {
		t.Fatal("expected watcher")
	}
	if len(h.watchersExcept("r1", "u1")) != 0 {
		t.Fatal("self should be excluded")
	}
	h.mx.Lock()
	unsub := h.dropUserWatchersLocked("u1")
	h.mx.Unlock()
	if h.hasRoomWatchers("r1") {
		t.Fatal("watcher remained after drop")
	}
	if len(unsub) != 1 {
		t.Fatalf("unsub=%v", unsub)
	}
}
