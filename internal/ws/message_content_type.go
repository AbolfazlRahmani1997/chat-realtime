package ws

import "strings"

// Message content kinds (stored in Mongo as message_type, JSON field "Type").
const (
	MessageContentTypeText  = "text"
	MessageContentTypeVoice = "voice"
	MessageContentTypeFile  = "file"
)

func normalizeMessageContentType(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case MessageContentTypeVoice:
		return MessageContentTypeVoice
	case MessageContentTypeFile:
		return MessageContentTypeFile
	default:
		return MessageContentTypeText
	}
}
