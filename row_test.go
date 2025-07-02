package sq

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/google/uuid"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/require"
)

func TestByteCast_1(t *testing.T) {
	data1 := []byte{1, 2}
	v1 := cast.ToInt64(data1)
	fmt.Println(v1)
}

func TestMapArray(t *testing.T) {
	var jsonstr = `
[
	{"name":"Apple",
	"loc": "GZ"},
	{"name":"Banana",
	"loc": "ZZ"}
]
`
	var jj []map[string]any
	err := json.Unmarshal([]byte(jsonstr), &jj)
	require.NoError(t, err)
}

func TestUUIDArrayJSON(t *testing.T) {
	u1 := uuid.NewString()
	u2 := uuid.NewString()
	var aa = []string{u1, u2}

	jb, err := json.Marshal(aa)
	require.NoError(t, err)

	var jk [][]byte
	err = json.Unmarshal(jb, &jk)
	require.NoError(t, err)
}

func TestUUIDGen(t *testing.T) {
	var u1 = [16]byte(uuid.New())
	var u2 = [16]byte(uuid.New())
	fmt.Println(u1, u2)
}

func TestArrayZero(t *testing.T) {
	var a1 [18]byte
	var a2 [18]byte

	assert.Equal(t, a1, a2)
}
