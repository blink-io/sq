package types

import "encoding/json"

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
