package admin

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"server/Dtos"
	DtoRepo "server/Dtos/DtoReporitories"
	"server/entity/Room"
	"server/enums"
	"server/helper"
	"server/internal/ws"
	"server/ports/services"
	"strconv"
)

type Handler struct {
	hub            *ws.Hub
	roomService    services.RoomServicePort
	messageService services.MessageServicePort
	wrapper        *helper.ResponserWrapper
}

func NewHandler(hub *ws.Hub, roomService services.RoomServicePort, message services.MessageServicePort) *Handler {
	return &Handler{roomService: roomService, messageService: message, hub: hub, wrapper: helper.NewResponseWrapper()}
}

func (receiver Handler) FindRoom(c *gin.Context) {

	room := receiver.roomService.RetrieveRoom(c.Param("roomId"))
	receiver.wrapper.SetResource(&room)
	c.JSON(200, receiver.wrapper.GetResource())

}

func (receiver Handler) FetchRooms(c *gin.Context) {
	page := 1
	if s := c.Query("page"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			page = v
		}
	}

	offset := 10
	if s := c.Query("offset"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			offset = v
		}
	} else if s := c.Query("limit"); s != "" {
		// keep compatibility with common pagination naming
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			offset = v
		}
	}

	memberId := c.Query("userId")
	roomId := c.Query("roomId")
	if roomId == "" {
		roomId = c.Query("id")
	}

	all := c.Query("all") == "true"

	dto := Dtos.GetAllRoomFilterDto{
		MemberId: memberId,
		Id:       roomId,
		All:      all,
	}

	rooms := receiver.roomService.FetchAllRooms(page, offset, dto)
	c.JSON(200, rooms)
}

func (receiver Handler) EditRoom(c *gin.Context) {
	dto := new(Dtos.UpdateRoomDto)

	err := c.BindJSON(dto)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	dto.Id = c.Param("roomId")
	fmt.Println(dto.Id)
	rooms := receiver.roomService.EditRooms(*dto)
	c.JSON(200, rooms)
}

func (receiver Handler) DeactiveRoom(c *gin.Context) {
	dto := Dtos.UpdateRoomDto{
		Id: c.Param("roomId"),
		Room: Room.Room{
			ActiveStatus: enums.Deactive,
		},
	}
	receiver.roomService.EditRooms(dto)

	receiver.hub.SetCachedRoomActiveStatus(c.Param("roomId"), string(enums.Deactive))

	c.JSON(200, gin.H{"message": "room deactivated"})
}

func (receiver Handler) ActiveRoom(c *gin.Context) {
	dto := Dtos.UpdateRoomDto{
		Id: c.Param("roomId"),
		Room: Room.Room{
			ActiveStatus: enums.Active,
		},
	}
	receiver.roomService.EditRooms(dto)

	receiver.hub.SetCachedRoomActiveStatus(c.Param("roomId"), string(enums.Active))

	c.JSON(200, gin.H{"message": "room activated"})
}

func (receiver Handler) GetMessage(c *gin.Context) {
	messageID := c.Param("messageId")
	if messageID == "" {
		// backward compatibility with the previous (buggy) implementation
		messageID = c.Query("id")
	}

	message := receiver.messageService.GetMessage(messageID)
	c.JSON(200, message)
}
func (receiver Handler) GetAllMessages(c *gin.Context) {
	page := 1
	if s := c.Query("page"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			page = v
		}
	}

	roomId := c.Query("roomId")
	if roomId == "" {
		roomId = c.Query("room_id")
	}
	if roomId == "" {
		// Backward compatibility with README/examples using `id` for filtering.
		roomId = c.Query("id")
	}

	clientId := c.Query("clientId")
	if clientId == "" {
		clientId = c.Query("client_id")
	}

	filter := DtoRepo.MessageFilterDto{
		RoomId:   roomId,
		ClientId: clientId,
	}

	messages := receiver.messageService.GetAllMessages(page, filter)
	c.JSON(200, messages)
}
