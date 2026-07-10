package ws

import "github.com/oklog/ulid/v2"

func newULID() string {
	return ulid.Make().String()
}
