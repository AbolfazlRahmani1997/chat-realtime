package ws

// Safe accessors for Hub.Rooms / Clients. All map mutations of Rooms go through
// these helpers or Hub.Run under h.mx so HTTP, Redis, and Broadcast do not race.

// EnsureRoom inserts room into the in-memory cache if missing. Idempotent.
func (h *Hub) EnsureRoom(room Room) {
	h.mx.Lock()
	defer h.mx.Unlock()
	if _, ok := h.Rooms[room.ID]; ok {
		return
	}
	clients := room.Clients
	if clients == nil {
		clients = make(map[string]*Client)
	}
	h.Rooms[room.ID] = &Room{
		_Id:          room._Id,
		ID:           room.ID,
		Name:         room.Name,
		Type:         room.Type,
		AvatarUrl:    room.AvatarUrl,
		Members:      room.Members,
		Message:      room.Message,
		Status:       room.Status,
		ActiveStatus: room.ActiveStatus,
		Clients:      clients,
		Pinned:       room.Pinned,
	}
}

// RoomExists reports whether the room is loaded in the Hub cache.
func (h *Hub) RoomExists(roomID string) bool {
	h.mx.RLock()
	defer h.mx.RUnlock()
	_, ok := h.Rooms[roomID]
	return ok
}

// GetRoomSnapshot returns a shallow copy of room metadata (no Clients map).
func (h *Hub) GetRoomSnapshot(roomID string) (Room, bool) {
	h.mx.RLock()
	defer h.mx.RUnlock()
	r, ok := h.Rooms[roomID]
	if !ok || r == nil {
		return Room{}, false
	}
	snap := *r
	snap.Clients = nil
	snap.Members = append([]Member(nil), r.Members...)
	return snap, true
}

// GetRoomClient returns the client pointer for a user in a room (may be nil).
func (h *Hub) GetRoomClient(roomID, userID string) (*Client, bool) {
	h.mx.RLock()
	defer h.mx.RUnlock()
	room, ok := h.Rooms[roomID]
	if !ok || room == nil {
		return nil, false
	}
	cl, ok := room.Clients[userID]
	return cl, ok
}

// PatchCachedRoom applies non-empty fields to a cached room.
func (h *Hub) PatchCachedRoom(roomID string, name, avatarURL string, roomType Type, members []Member) {
	h.mx.Lock()
	defer h.mx.Unlock()
	cached, ok := h.Rooms[roomID]
	if !ok || cached == nil {
		return
	}
	if name != "" {
		cached.Name = name
	}
	if avatarURL != "" {
		cached.AvatarUrl = avatarURL
	}
	if roomType != "" {
		cached.Type = roomType
	}
	if members != nil {
		cached.Members = members
	}
}

// SetCachedRoomAvatar updates avatar on a cached room.
func (h *Hub) SetCachedRoomAvatar(roomID, avatarURL string) {
	h.mx.Lock()
	defer h.mx.Unlock()
	if cached, ok := h.Rooms[roomID]; ok && cached != nil {
		cached.AvatarUrl = avatarURL
	}
}

// SetCachedRoomMembers replaces members on a cached room.
func (h *Hub) SetCachedRoomMembers(roomID string, members []Member) {
	h.mx.Lock()
	defer h.mx.Unlock()
	if cached, ok := h.Rooms[roomID]; ok && cached != nil {
		cached.Members = members
	}
}

// SetCachedRoomActiveStatus updates ActiveStatus on a cached room.
func (h *Hub) SetCachedRoomActiveStatus(roomID, activeStatus string) {
	h.mx.Lock()
	defer h.mx.Unlock()
	if cached, ok := h.Rooms[roomID]; ok && cached != nil {
		cached.ActiveStatus = activeStatus
	}
}

// ListRoomClientRes returns online client summaries for a room.
func (h *Hub) ListRoomClientRes(roomID string) []ClientRes {
	h.mx.RLock()
	defer h.mx.RUnlock()
	room, ok := h.Rooms[roomID]
	if !ok || room == nil {
		return []ClientRes{}
	}
	out := make([]ClientRes, 0, len(room.Clients))
	for _, cl := range room.Clients {
		if cl == nil {
			continue
		}
		out = append(out, ClientRes{ID: cl.ID, Username: cl.Username})
	}
	return out
}

// broadcastSnapshot holds data copied under the Hub lock for async fan-out.
// in-room membership is derived from clients (no per-broadcast map alloc).
type broadcastSnapshot struct {
	roomID  string
	members []Member
	clients []*Client
}

func (s *broadcastSnapshot) hasClient(userID string) bool {
	for _, cl := range s.clients {
		if cl != nil && cl.ID == userID {
			return true
		}
	}
	return false
}

func (h *Hub) snapshotRoomForBroadcast(roomID string) (broadcastSnapshot, bool) {
	h.mx.RLock()
	defer h.mx.RUnlock()
	room, ok := h.Rooms[roomID]
	if !ok || room == nil {
		return broadcastSnapshot{}, false
	}
	snap := broadcastSnapshot{
		roomID:  roomID,
		members: append([]Member(nil), room.Members...),
		clients: make([]*Client, 0, len(room.Clients)),
	}
	for _, cl := range room.Clients {
		if cl != nil {
			snap.clients = append(snap.clients, cl)
		}
	}
	return snap, true
}

// snapshotMembersFromDB loads room members when the room is not cached locally
// (typical for cross-instance Redis fan-out to users who have not joined the room).
func (h *Hub) snapshotMembersFromDB(roomID string) (broadcastSnapshot, bool) {
	if h.DB == nil || roomID == "" {
		return broadcastSnapshot{}, false
	}
	room := h.DB.RoomOps().GetById(roomID)
	if room.ID == "" {
		return broadcastSnapshot{}, false
	}
	return broadcastSnapshot{
		roomID:  roomID,
		members: append([]Member(nil), room.Members...),
		clients: nil,
	}, true
}

func (h *Hub) getUserLocked(userID string) (*User, bool) {
	h.mx.RLock()
	defer h.mx.RUnlock()
	u, ok := h.Users[userID]
	return u, ok
}

// UserExists reports whether the user is currently tracked in the Hub.
func (h *Hub) UserExists(userID string) bool {
	_, ok := h.getUserLocked(userID)
	return ok
}

// CollectUnifiedClientsForConn returns unified room clients that still hold connectionID.
func (h *Hub) CollectUnifiedClientsForConn(userID, connectionID string) []*Client {
	h.mx.RLock()
	candidates := make([]*Client, 0)
	for _, room := range h.Rooms {
		if room == nil {
			continue
		}
		cl, ok := room.Clients[userID]
		if ok && cl != nil && cl.UnifiedConn {
			candidates = append(candidates, cl)
		}
	}
	h.mx.RUnlock()

	out := make([]*Client, 0, len(candidates))
	for _, cl := range candidates {
		cl.mu.Lock()
		_, has := cl.Conn[connectionID]
		cl.mu.Unlock()
		if has {
			out = append(out, cl)
		}
	}
	return out
}
