package Dtos

import (
	"go.mongodb.org/mongo-driver/bson"
	"server/entity/Room"
)

type GetAllRoomDto struct {
}

type GetAllRoomFilterDto struct {
	Id       string `json:"Id,"`
	MemberId string `json:"MemberId"`
	All      bool   `json:"all"`
}

func (receiver GetAllRoomFilterDto) GetFilter() bson.M {
	filters := bson.M{}

	if !receiver.All {
		filters["active_status"] = bson.M{"$ne": "deactive"}
	}

	if receiver.MemberId != "" {
		filters["members.id"] = receiver.MemberId
	}
	if receiver.Id != "" {
		filters["id"] = receiver.Id

	}
	return filters
}

type UpdateRoomDto struct {
	Id   string    `json:"Id,omitempty"`
	Room Room.Room `json:"Room,omitempty"`
}

func (receiver UpdateRoomDto) GetUpdate() bson.D {
	filters := bson.D{}
	if receiver.Room.Name != "" {
		filters = append(filters, bson.E{Key: "name", Value: receiver.Room.Name})
	}
	if receiver.Room.Status != "" {
		filters = append(filters, bson.E{Key: "status", Value: receiver.Room.Status})
	}
	if receiver.Room.ActiveStatus != "" {
		filters = append(filters, bson.E{Key: "active_status", Value: receiver.Room.ActiveStatus})
	}
	return filters

}

type MessageEntity struct {
}

type RoomResource struct {
}

type AllRoomRequest struct {
}
