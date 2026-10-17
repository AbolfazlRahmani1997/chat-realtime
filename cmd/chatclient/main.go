// Interactive client for the unified chat WebSocket.
//
//	go run ./cmd/chatclient -token dev-token-user1 -create -room demo
//	go run ./cmd/chatclient -token dev-token-user2 -room demo
//
// Lines you type are sent to the current room. Commands:
//
//	/join <room>   /leave   /history   /rooms   /ping   /quit
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/oklog/ulid/v2"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "chat HTTP address")
	token := flag.String("token", "dev-token-user1", "auth token (mock user when MOCK_USERS_ENABLED=1)")
	room := flag.String("room", "", "room id to join after connect")
	create := flag.Bool("create", false, "POST /chat/api/createRoom for -room before connecting")
	name := flag.String("name", "demo", "room name used with -create")
	members := flag.String("members", "1,2", "comma-separated member ids used with -create")
	flag.Parse()

	if *create {
		if *room == "" {
			fmt.Fprintln(os.Stderr, "-create needs -room")
			os.Exit(1)
		}
		if err := createRoom(*addr, *room, *name, splitIDs(*members)); err != nil {
			fmt.Fprintf(os.Stderr, "create room: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("created room %s\n", *room)
	}

	wsURL := url.URL{Scheme: "ws", Host: *addr, Path: "/chat/ws", RawQuery: "token=" + url.QueryEscape(*token)}
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL.String(), nil)
	if err != nil {
		if resp != nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			resp.Body.Close()
			fmt.Fprintf(os.Stderr, "dial %s: %v (%s %s)\n", wsURL.String(), err, resp.Status, bytes.TrimSpace(body))
		} else {
			fmt.Fprintf(os.Stderr, "dial %s: %v\n", wsURL.String(), err)
		}
		os.Exit(1)
	}
	defer conn.Close()
	fmt.Printf("connected %s\n", wsURL.String())

	current := *room
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				fmt.Printf("\nread: %v\n", err)
				return
			}
			fmt.Printf("\n← %s\n> ", pretty(data))
		}
	}()

	if current != "" {
		if err := writeEvent(conn, map[string]string{"event_type": "joinRoom", "room_id": current, "page": "1"}); err != nil {
			fmt.Fprintf(os.Stderr, "join: %v\n", err)
			return
		}
		fmt.Printf("joined %s\n", current)
	}
	fmt.Println("type a message, or /join /leave /history /rooms /ping /quit")

	in := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	for in.Scan() {
		line := strings.TrimSpace(in.Text())
		if line == "" {
			fmt.Print("> ")
			continue
		}
		fields := strings.Fields(line)
		switch fields[0] {
		case "/quit":
			return
		case "/rooms":
			err = writeEvent(conn, map[string]string{"event_type": "listRooms", "item": "1"})
		case "/ping":
			if current == "" {
				fmt.Println("join a room first")
				fmt.Print("> ")
				continue
			}
			err = writeEvent(conn, map[string]string{"event_type": "sendMessage", "room_id": current, "ulid": "ping", "content": ""})
		case "/history":
			if current == "" {
				fmt.Println("join a room first")
				fmt.Print("> ")
				continue
			}
			err = writeEvent(conn, map[string]string{"event_type": "roomHistory", "room_id": current, "item": "1"})
		case "/leave":
			if current == "" {
				fmt.Print("> ")
				continue
			}
			err = writeEvent(conn, map[string]string{"event_type": "leaveRoom", "room_id": current})
			current = ""
		case "/join":
			if len(fields) < 2 {
				fmt.Println("usage: /join <roomId>")
				fmt.Print("> ")
				continue
			}
			current = fields[1]
			err = writeEvent(conn, map[string]string{"event_type": "joinRoom", "room_id": current, "page": "1"})
		default:
			if current == "" {
				fmt.Println("join a room first: /join <roomId>")
				fmt.Print("> ")
				continue
			}
			err = writeEvent(conn, map[string]string{
				"event_type": "sendMessage",
				"room_id":    current,
				"content":    line,
				"ulid":       ulid.Make().String(),
				"type":       "text",
			})
		}
		if err != nil {
			fmt.Printf("write: %v\n", err)
			return
		}
		select {
		case <-done:
			return
		default:
		}
		fmt.Print("> ")
	}
}

func writeEvent(conn *websocket.Conn, payload map[string]string) error {
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return conn.WriteJSON(payload)
}

func createRoom(addr, id, name string, memberIDs []string) error {
	members := make([]map[string]any, 0, len(memberIDs))
	for _, id := range memberIDs {
		members = append(members, map[string]any{
			"Id":           id,
			"firstname":    id,
			"lastname":     "",
			"Notification": true,
		})
	}
	body, err := json.Marshal(map[string]any{
		"Id":     id,
		"Name":   name,
		"Type":   "GROUP",
		"Member": members,
		"Owner":  memberIDs,
		"Writer": memberIDs,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/chat/api/createRoom", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
	if res.StatusCode >= 300 {
		return fmt.Errorf("%s %s", res.Status, bytes.TrimSpace(raw))
	}
	return nil
}

func splitIDs(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func pretty(data []byte) string {
	var buf bytes.Buffer
	if err := json.Indent(&buf, data, "", "  "); err != nil {
		return string(data)
	}
	return buf.String()
}
