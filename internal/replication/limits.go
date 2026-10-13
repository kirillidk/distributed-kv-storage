package replication

// Reserve enough protobuf framing for a payload and a full uint64 offset.
const (
	MaxPayloadSize = 4 * 1024 * 1024
	MaxMessageSize = MaxPayloadSize + 16
)
