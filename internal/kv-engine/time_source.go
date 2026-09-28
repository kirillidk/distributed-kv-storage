package kvengine

import (
	"time"
)

type TimeSource interface {
	Now() uint64
}

type SystemTimeSource struct{}

func (s SystemTimeSource) Now() uint64 {
	return uint64(time.Now().Unix())
}
