package util

import (
	"crypto/rand"
	"io"
	"time"

	"github.com/oklog/ulid/v2"
)

var entropy = ulid.Monotonic(reader{}, 0)

type reader struct{}

func (reader) Read(p []byte) (int, error) {
	return io.ReadFull(rand.Reader, p)
}

func NewULID() (string, error) {
	id, err := ulid.New(ulid.Timestamp(time.Now().UTC()), entropy)
	if err != nil {
		return "", err
	}
	return id.String(), nil
}
