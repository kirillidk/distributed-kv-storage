package sharding

import "testing"

func TestForKey(t *testing.T) {
	for _, tc := range []struct {
		key         []byte
		count, want uint32
	}{
		{[]byte("alpha"), 1, 0},
		{[]byte("alpha"), 2, 0},
		{[]byte("beta"), 2, 1},
		{[]byte("123456789"), 7, 5},
		{[]byte{0, 0xff, 1}, 7, 3},
	} {
		if got := ForKey(tc.key, tc.count); got != tc.want {
			t.Errorf("ForKey(%x, %d) = %d, want %d", tc.key, tc.count, got, tc.want)
		}
	}
}
