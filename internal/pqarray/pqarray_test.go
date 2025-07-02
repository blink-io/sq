package pqarray

import (
	"database/sql"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStringArray(t *testing.T) {
	var strarr StringArray

	err := strarr.Scan(`{"training", "presentation"}`)
	require.NoError(t, err)
}

func TestGeneriArray(t *testing.T) {
	var str = []byte(`{"2025-07-01T22:13:14.885561471+08:00","2025-07-01T22:13:14.885561501+08:00"}`)

	var tt = new([]sql.NullTime)
	arr := Array(tt)
	err := arr.Scan(string(str))
	require.NoError(t, err)
}
