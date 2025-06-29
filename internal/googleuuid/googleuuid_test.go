package googleuuid

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEncodeHex(t *testing.T) {
	uuidstr := "7af71857-21e3-483f-87da-437d9f5beb7e"
	bb, err := Parse(uuidstr)
	require.NoError(t, err)

	var buf [36]byte
	EncodeHex(buf[:], bb)
	require.Equal(t, uuidstr, string(buf[:]))
}
