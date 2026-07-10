package reporisoties

import (
	"server/Dtos"
	"server/entity/Room"
)

type RoomRepositoryPort interface {
	GetRoom(id string) Room.Room
	GetAllRooms(page int, offset int, filter Dtos.GetAllRoomFilterDto) []Room.Room
	Update(Update Dtos.UpdateRoomDto) []Room.Room
}
