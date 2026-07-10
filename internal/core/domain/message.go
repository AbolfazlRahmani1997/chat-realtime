package domain

import "time"

type ReplyInfo struct {
	ID       string `json:"id"`
	Content  string `json:"content"`
	Username string `json:"username"`
}

type ForwardInfo struct {
	FromRoomID string `json:"from_room_id"`
	FromMsgID  string `json:"from_msg_id"`
	Content    string `json:"content"`
	Username   string `json:"username"`
}

type Message struct {
	ID          string       `json:"_id"`
	Content     string       `json:"Content,omitempty"`
	Type        string       `json:"Type,omitempty"`
	UniqId      string       `json:"UniqId"`
	RoomID      string       `json:"RoomID,omitempty"`
	Username    string       `json:"Username,omitempty"`
	ClientID    string       `json:"ClientID,omitempty"`
	Deliver     []string     `json:"Deliver"`
	Read        []string     `json:"Read"`
	ReplyTo     *ReplyInfo   `json:"ReplyTo,omitempty"`
	ForwardFrom *ForwardInfo `json:"ForwardFrom,omitempty"`
	CreatedAt   time.Time    `json:"CreatedAt"`
	UpdatedAt   time.Time    `json:"UpdatedAt"`
}
