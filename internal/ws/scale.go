package ws

import (
	"hash/fnv"
	"os"
	"strconv"
)

const (
	hubPersistBuffer  = 8192
	hubPersistWorkers = 8
	hubReceiptWorkers = 8
	defaultMsgShards  = 64
	defaultMaxWSConns = 25000
)

// persistJob is a copy of a message so the broadcast worker can fan out
// without waiting on Kafka, Mongo, or Redis.
type persistJob struct {
	msg Message
}

func (h *Hub) ensureScaleWorkers() {
	h.workerOnce.Do(func() {
		if h.roomWatchers == nil {
			h.roomWatchers = make(map[string]map[string]struct{})
		}
		if h.userRooms == nil {
			h.userRooms = make(map[string]map[string]struct{})
		}
		if h.shardRooms == nil {
			h.shardRooms = make(map[int]int)
		}
		if h.persistCh == nil {
			h.persistCh = make(chan persistJob, hubPersistBuffer)
		}
		for i := 0; i < hubBroadcastWorkers; i++ {
			go h.broadcastWorker()
		}
		for i := 0; i < hubPersistWorkers; i++ {
			go h.persistWorker()
		}
		for i := 0; i < hubReceiptWorkers; i++ {
			go h.receiptWorker()
		}
	})
}

func (h *Hub) persistWorker() {
	for job := range h.persistCh {
		h.persistMessage(job.msg)
	}
}

func (h *Hub) receiptWorker() {
	for rec := range h.ReadAble {
		h.handleReadReceipt(rec)
	}
}

// enqueuePersist keeps fan-out off the storage path. A full queue falls back
// to inline persistence so a message is not dropped.
func (h *Hub) enqueuePersist(m Message) {
	if h.persistCh == nil {
		h.persistMessage(m)
		return
	}
	select {
	case h.persistCh <- persistJob{msg: m}:
	default:
		h.persistMessage(m)
	}
}

func (h *Hub) acquireConn() bool {
	max := maxLiveConnections()
	for {
		cur := h.liveConns.Load()
		if max > 0 && cur >= max {
			return false
		}
		if h.liveConns.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func (h *Hub) releaseConn() {
	h.liveConns.Add(^uint64(0)) // -1
}

func maxLiveConnections() uint64 {
	v := os.Getenv("MAX_WS_CONNECTIONS")
	if v == "" {
		return defaultMaxWSConns
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return defaultMaxWSConns
	}
	return n
}

func messageShardCount() uint32 {
	v := os.Getenv("REDIS_MESSAGE_SHARDS")
	if v == "" {
		return defaultMsgShards
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return defaultMsgShards
	}
	return uint32(n)
}

func messageShard(roomID string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(roomID))
	return int(h.Sum32() % messageShardCount())
}

func messageChannel(roomID string) string {
	return "chat:messages:" + strconv.Itoa(messageShard(roomID))
}

// NoteRoomWatchers records that this process has a connected user who can
// receive traffic for these rooms, and subscribes only those Redis shards.
func (h *Hub) NoteRoomWatchers(userID string, roomIDs []string) {
	if userID == "" || len(roomIDs) == 0 {
		return
	}
	h.mx.Lock()
	if h.roomWatchers == nil {
		h.roomWatchers = make(map[string]map[string]struct{})
	}
	if h.userRooms == nil {
		h.userRooms = make(map[string]map[string]struct{})
	}
	if h.shardRooms == nil {
		h.shardRooms = make(map[int]int)
	}
	mine := h.userRooms[userID]
	if mine == nil {
		mine = make(map[string]struct{})
		h.userRooms[userID] = mine
	}
	var subscribe []string
	for _, roomID := range roomIDs {
		if roomID == "" {
			continue
		}
		if _, ok := mine[roomID]; ok {
			continue
		}
		mine[roomID] = struct{}{}
		set := h.roomWatchers[roomID]
		if set == nil {
			set = make(map[string]struct{})
			h.roomWatchers[roomID] = set
		}
		if _, ok := set[userID]; ok {
			continue
		}
		set[userID] = struct{}{}
		if len(set) != 1 {
			continue
		}
		shard := messageShard(roomID)
		h.shardRooms[shard]++
		if h.shardRooms[shard] == 1 {
			subscribe = append(subscribe, messageChannel(roomID))
		}
	}
	broker := h.RedisBroker
	h.mx.Unlock()
	if broker != nil {
		broker.SubscribeMessages(subscribe)
	}
}

// dropUserWatchersLocked removes the user's room interest. Caller holds h.mx.
// Returned channels are Redis shards that no longer have a local room.
func (h *Hub) dropUserWatchersLocked(userID string) []string {
	if h.userRooms == nil {
		return nil
	}
	mine := h.userRooms[userID]
	delete(h.userRooms, userID)
	var unsub []string
	for roomID := range mine {
		set := h.roomWatchers[roomID]
		if set == nil {
			continue
		}
		delete(set, userID)
		if len(set) > 0 {
			continue
		}
		delete(h.roomWatchers, roomID)
		shard := messageShard(roomID)
		if h.shardRooms[shard] > 0 {
			h.shardRooms[shard]--
		}
		if h.shardRooms[shard] == 0 {
			delete(h.shardRooms, shard)
			unsub = append(unsub, messageChannel(roomID))
		}
	}
	return unsub
}

func (h *Hub) hasRoomWatchers(roomID string) bool {
	h.mx.RLock()
	defer h.mx.RUnlock()
	return len(h.roomWatchers[roomID]) > 0
}

func (h *Hub) watchersExcept(roomID, except string) []*User {
	h.mx.RLock()
	defer h.mx.RUnlock()
	set := h.roomWatchers[roomID]
	if len(set) == 0 {
		return nil
	}
	out := make([]*User, 0, len(set))
	for uid := range set {
		if uid == except || uid == "" {
			continue
		}
		if u := h.Users[uid]; u != nil {
			out = append(out, u)
		}
	}
	return out
}

// roomIDsFromResponses collects room ids from a room list payload.
func roomIDsFromResponses(rooms []RoomResponse) []string {
	ids := make([]string, 0, len(rooms))
	for _, room := range rooms {
		if room.Id != "" {
			ids = append(ids, room.Id)
		}
	}
	return ids
}
