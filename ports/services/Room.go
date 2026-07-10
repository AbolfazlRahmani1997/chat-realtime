package services

import (
	"server/Dtos"
	"server/entity/Room"
)

type RoomServicePort interface {
	RetrieveRoom(id string) Room.Room
	FetchAllRooms(page int, offset int, filter Dtos.GetAllRoomFilterDto) []Room.Room
	EditRooms(filter Dtos.UpdateRoomDto) []Room.Room
}
