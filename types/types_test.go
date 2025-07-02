package types

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIsZeroUUID(t *testing.T) {
	var u1 [16]byte
	var u2 [16]byte

	i1 := IsZeroUUID(u1)
	i2 := IsZeroUUID(u2)

	require.True(t, i1)
	require.True(t, i2)
}
