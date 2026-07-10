package Services

import (
	"fmt"
	"server/Dtos"
	"server/entity/Room"
	"server/ports/reporisoties"
	"server/ports/services"
)

type RoomService struct {
	RoomRepository reporisoties.RoomRepositoryPort
}

func NewRoomService(RoomRepository reporisoties.RoomRepositoryPort) services.RoomServicePort {

	return RoomService{RoomRepository: RoomRepository}

}
func (receiver RoomService) RetrieveRoom(id string) Room.Room {
	fmt.Println(id)
	room := receiver.RoomRepository.GetRoom(id)

	return room
}

func (receiver RoomService) FetchAllRooms(page int, offset int, filter Dtos.GetAllRoomFilterDto) []Room.Room {

	rooms := receiver.RoomRepository.GetAllRooms(page, offset, filter)

	return rooms
}

func (receiver RoomService) EditRooms(filter Dtos.UpdateRoomDto) []Room.Room {

	rooms := receiver.RoomRepository.Update(filter)

	return rooms
}
