package domain

type RoomType string

const (
	PRIVATE RoomType = "PRIVATE"
	GROUP   RoomType = "GROUP"
	Support RoomType = "Support"
	TEMP    RoomType = "TEMP"
	CHANEL  RoomType = "CHANEL"
	BOT     RoomType = "BOT"
)

type Room struct {
	ID        string       `json:"Id"`
	Name      string       `json:"Name"`
	Type      RoomType     `json:"Type"`
	AvatarUrl string       `json:"AvatarUrl"`
	Members   []Member     `json:"Members"`
	Message   Message      `json:"Message"`
	Status    MemberStatus `json:"Status,omitempty"`
	Pinned    bool         `json:"Pinned"`
}

type Member struct {
	Id           string   `json:"Id"`
	Roles        []string `json:"roles"`
	FirstName    string   `json:"firstname"`
	LastName     string   `json:"lastname"`
	AvatarUrl    string   `json:"AvatarUrl"`
	Notification bool     `json:"Notification"`
	Pin          bool     `json:"Pin"`
	LastSeen     int64    `json:"LastSeen,omitempty"`
}

type MemberStatus string

const (
	Online  MemberStatus = "online"
	Offline MemberStatus = "offline"
	Evade   MemberStatus = "evade"
)
