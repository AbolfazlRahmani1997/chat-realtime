package ws

import "strings"

func isMember(userID string, members []Member) bool {
	for _, m := range members {
		if m.Id == userID {
			return true
		}
	}
	return false
}

func canWrite(userID string, members []Member) bool {
	for _, m := range members {
		if m.Id != userID {
			continue
		}
		for _, role := range m.Roles {
			// "Owner" implies write permission too.
			if role == "Owner" || role == "Writer" {
				return true
			}
		}
	}
	return false
}

func normalizeRoomType(raw string) Type {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}

	// Accept user-provided spellings (e.g. "chanel" vs "channel").
	if strings.EqualFold(raw, "chanel") || strings.EqualFold(raw, "channel") {
		return CHANEL
	}
	if strings.EqualFold(raw, "private") {
		return PRIVATE
	}
	if strings.EqualFold(raw, "group") {
		return GROUP
	}
	if strings.EqualFold(raw, "temp") || strings.EqualFold(raw, "temporary") {
		return TEMP
	}
	if strings.EqualFold(raw, "bot") {
		return BOT
	}

	// Fall back to raw upper-cased token (in case you store custom types).
	return Type(strings.ToUpper(raw))
}
