package main

import (
	ScyllaRepo "server/adapters/Reporisotires/ScyllaDB"
	"server/internal/ws"
	"server/ports/reporisoties"
)

func initScyllaProvider() ws.DatabaseProvider {
	session := ScyllaRepo.NewSession()

	return ws.NewScyllaProvider(session)
}

func initScyllaRepos() (reporisoties.RoomRepositoryPort, reporisoties.MessageRepositoryPort) {
	session := ScyllaRepo.NewSession()

	roomRepo := ScyllaRepo.NewRoomRepository(session)
	messageRepo := ScyllaRepo.NewMessageRepository(session)

	return roomRepo, messageRepo
}
