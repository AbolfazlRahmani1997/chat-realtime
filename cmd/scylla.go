package main

import (
	ScyllaRepo "server/adapters/Reporisotires/ScyllaDB"
	"server/internal/ws"
	"server/ports/reporisoties"

	"github.com/gocql/gocql"
)

// initScylla wires one shared session for Hub provider + admin repositories.
func initScylla() (ws.DatabaseProvider, reporisoties.RoomRepositoryPort, reporisoties.MessageRepositoryPort, *gocql.Session) {
	session := ScyllaRepo.NewSession()
	return ws.NewScyllaProvider(session),
		ScyllaRepo.NewRoomRepository(session),
		ScyllaRepo.NewMessageRepository(session),
		session
}
