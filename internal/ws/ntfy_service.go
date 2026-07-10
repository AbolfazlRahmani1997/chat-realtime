package ws

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

type NtfyService struct {
	baseURL     string
	topicPrefix string
	client      *http.Client
}

type NtfyMessage struct {
	Topic    string            `json:"topic"`
	Title    string            `json:"title"`
	Message  string            `json:"message"`
	Tags     []string          `json:"tags,omitempty"`
	Priority int               `json:"priority,omitempty"`
	Actions  []NtfyAction      `json:"actions,omitempty"`
	Attach   string            `json:"attach,omitempty"`
	Filename string            `json:"filename,omitempty"`
	Click    string            `json:"click,omitempty"`
	Headers  map[string]string `json:"-"`
}

type NtfyAction struct {
	Action string `json:"action"`
	Label  string `json:"label"`
	URL    string `json:"url"`
	Clear  bool   `json:"clear"`
}

func NewNtfyService() *NtfyService {
	baseURL := os.Getenv("NTFY_URL")
	if baseURL == "" {
		return nil
	}
	prefix := os.Getenv("NTFY_TOPIC_PREFIX")
	if prefix == "" {
		prefix = "chat"
	}
	return &NtfyService{
		baseURL:     baseURL,
		topicPrefix: prefix,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (n *NtfyService) privateTopic(userId string) string {
	return fmt.Sprintf("%s-%s", n.topicPrefix, userId)
}

func (n *NtfyService) generalTopic() string {
	return fmt.Sprintf("%s-general", n.topicPrefix)
}

func (n *NtfyService) send(msg *NtfyMessage) {
	if n == nil {
		return
	}
	body, err := json.Marshal(msg)
	if err != nil {
		log.Printf("ntfy: marshal error: %v", err)
		return
	}
	url := fmt.Sprintf("%s/%s", n.baseURL, msg.Topic)
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		log.Printf("ntfy: request error: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range msg.Headers {
		req.Header.Set(k, v)
	}
	resp, err := n.client.Do(req)
	if err != nil {
		log.Printf("ntfy: send error: %v", err)
		return
	}
	_ = resp.Body.Close()
}

func (n *NtfyService) SendPrivateNotification(userId, title, message, roomId, clickURL string) {
	n.send(&NtfyMessage{
		Topic:   n.privateTopic(userId),
		Title:   title,
		Message: message,
		Tags:    []string{"envelope"},
		Actions: []NtfyAction{
			{Action: "view", Label: "مشاهده پیام", URL: clickURL, Clear: true},
		},
		Click: clickURL,
	})
}

func (n *NtfyService) SendGeneralNotification(title, message, clickURL string) {
	n.send(&NtfyMessage{
		Topic:   n.generalTopic(),
		Title:   title,
		Message: message,
		Tags:    []string{"mega"},
		Click:   clickURL,
	})
}

func (n *NtfyService) NotifyNewMessage(userId, senderName, content, roomId, msgType string, clickURL string) {
	preview := content
	if msgType == "voice" {
		preview = "🎤 پیام صوتی"
	} else if msgType == "file" {
		preview = "📎 فایل"
	}
	if len([]rune(preview)) > 100 {
		runes := []rune(preview)
		preview = string(runes[:100]) + "…"
	}
	title := fmt.Sprintf("پیام جدید از %s", senderName)
	n.SendPrivateNotification(userId, title, preview, roomId, clickURL)
}

func (n *NtfyService) NotifyNewRoom(userId, roomName string) {
	title := "اتاق جدید"
	message := fmt.Sprintf("شما به اتاق «%s» اضافه شدید", roomName)
	n.SendPrivateNotification(userId, title, message, "", "")
}

func (n *NtfyService) NotifyGeneral(event string, clickURL string) {
	n.SendGeneralNotification("اعلان چت", event, clickURL)
}
