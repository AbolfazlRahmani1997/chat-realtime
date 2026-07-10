package ws

import (
	"encoding/json"
	"fmt"
	"os"
)

// LoadMockUsers reads mock users from JSON file.
// Returns nil if MOCK_USERS_ENABLED is not "true" or file not found.
func LoadMockUsers() map[string]UserRequest {
	if os.Getenv("MOCK_USERS_ENABLED") != "1" {
		return nil
	}
	fmt.Println("LoadMockUsers")
	path := os.Getenv("MOCK_USERS_FILE")

	if path == "" {
		path = "/config/mock_users.json"
	}
	fmt.Println("LoadMockUsers", path)
	data, err := os.ReadFile(path)
	fmt.Println("LoadMockUsers", string(data), err)
	if err != nil {
		return nil
	}
	var m map[string]UserRequest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil
	}
	return m
}
