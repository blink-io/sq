package sq

import (
	"fmt"
	"testing"

	"github.com/spf13/cast"
)

func TestByteCast_1(t *testing.T) {
	data1 := []byte{1, 2}
	v1 := cast.ToInt64(data1)
	fmt.Println(v1)
}
