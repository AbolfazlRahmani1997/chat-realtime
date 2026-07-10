package domain

type User struct {
	UserId    string `json:"Id"`
	UserName  string `json:"Username"`
	FirstName string `json:"Firstname"`
	LastName  string `json:"LastName"`
	AvatarUrl string `json:"AvatarUrl"`
	LastSeen  int64  `json:"LastSeen,omitempty"`
}

type UserSession struct {
	UserId       string
	ConnectionId string
	Conn         ConnWriter
}

type ConnWriter interface {
	WriteJSON(interface{}) error
	Close() error
	ReadMessage() (int, []byte, error)
}
