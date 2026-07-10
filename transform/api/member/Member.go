package member

type Member struct {
	Id        string   `json:"Id"`
	Roles     []string `json:"roles"`
	FirstName string   `json:"firstname"`
	LastName  string   `json:"lastname"`
	AvatarUrl string   `json:"AvatarUrl"bson:"avatar_url"`
}

func (receiver *Member) Get() {

}
