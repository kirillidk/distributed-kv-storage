package kvengine

type TestTimeSource struct {
	t uint64
}

func (t *TestTimeSource) Now() uint64 {
	return t.t
}

func (t *TestTimeSource) SetTime(time uint64) {
	t.t = time
}

func (t *TestTimeSource) AdvanceTime(delta uint64) {
	t.t += delta
}
