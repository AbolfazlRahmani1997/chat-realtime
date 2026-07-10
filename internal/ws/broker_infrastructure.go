package ws

type RoomBrokerInfrastructure interface {
	Consume()
}

type RoomBrokerDto struct {
	Id     string   `json:"Id"`
	Name   string   `json:"Name"`
	Member []Member `json:"Member"`
	Type   Type     `json:"type" `
}
