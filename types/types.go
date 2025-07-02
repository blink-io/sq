package types

import (
	"bytes"
	"encoding/json"
)

var zeroUUID [16]byte

func IsZeroUUID(u [16]byte) bool {
	return bytes.Equal(u[:], zeroUUID[:])
}

type JSON map[string]any

func (j JSON) Unmarshal(target any) error {
	bytes, err := json.Marshal(target)
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, target)
}

func (j JSON) Bytes() ([]byte, error) {
	return json.Marshal(j)
}
