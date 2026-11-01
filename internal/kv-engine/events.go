package kvengine

import (
	"encoding/binary"
	"errors"

	"github.com/kirillidk/distributed-kv-storage/api/replication"
)

const (
	setEventMagic     uint32 = 0x6f825fba
	addEventMagic     uint32 = 0x6f825fbb
	replaceEventMagic uint32 = 0x6f825fbc
	deleteEventMagic  uint32 = 0x6f825fbd
	incrEventMagic    uint32 = 0x6f825fbe
	decrEventMagic    uint32 = 0x6f825fbf
)

// Set keeps its existing 20-byte header for compatibility with persisted WALs.
// Other commands use: type:u32 | time:u64 | expiresAt:u64 | delta:i64 |
// keySize:u32 | valueSize:u32 | key | value. All integers are big-endian.
// time fixes the expiry check at command creation; replay never uses wall time.
type mutation struct {
	kind           uint32
	now, expiresAt uint64
	delta          int64
	key, value     []byte
}

func encodeMutation(cmd mutation) ([]byte, error) {
	headerSize := 36
	if cmd.kind == setEventMagic {
		headerSize = 20
	}
	if uint64(len(cmd.key))+uint64(len(cmd.value))+uint64(headerSize) > replication.MaxPayloadSize {
		return nil, errors.New("mutation payload is too large")
	}
	payload := make([]byte, headerSize+len(cmd.key)+len(cmd.value))
	binary.BigEndian.PutUint32(payload, cmd.kind)
	if cmd.kind == setEventMagic {
		binary.BigEndian.PutUint64(payload[4:], cmd.expiresAt)
	} else {
		binary.BigEndian.PutUint64(payload[4:], cmd.now)
		binary.BigEndian.PutUint64(payload[12:], cmd.expiresAt)
		binary.BigEndian.PutUint64(payload[20:], uint64(cmd.delta))
	}
	binary.BigEndian.PutUint32(payload[headerSize-8:], uint32(len(cmd.key)))
	binary.BigEndian.PutUint32(payload[headerSize-4:], uint32(len(cmd.value)))
	copy(payload[headerSize:], cmd.key)
	copy(payload[headerSize+len(cmd.key):], cmd.value)
	return payload, nil
}

func encodeSetEvent(key, value []byte, expiresAt uint64) ([]byte, error) {
	return encodeMutation(mutation{kind: setEventMagic, key: key, value: value, expiresAt: expiresAt})
}

func decodeMutation(payload []byte) (mutation, error) {
	var cmd mutation
	if len(payload) < 4 {
		return cmd, errors.New("malformed payload")
	}
	cmd.kind = binary.BigEndian.Uint32(payload)
	headerSize := 36
	switch cmd.kind {
	case setEventMagic:
		headerSize = 20
	case addEventMagic, replaceEventMagic, deleteEventMagic, incrEventMagic, decrEventMagic:
	default:
		return cmd, errors.New("invalid command type")
	}
	if len(payload) < headerSize || len(payload) > replication.MaxPayloadSize {
		return cmd, errors.New("malformed payload")
	}
	if cmd.kind == setEventMagic {
		cmd.expiresAt = binary.BigEndian.Uint64(payload[4:])
	} else {
		cmd.now = binary.BigEndian.Uint64(payload[4:])
		cmd.expiresAt = binary.BigEndian.Uint64(payload[12:])
		cmd.delta = int64(binary.BigEndian.Uint64(payload[20:]))
	}
	keySize := uint64(binary.BigEndian.Uint32(payload[headerSize-8:]))
	valueSize := uint64(binary.BigEndian.Uint32(payload[headerSize-4:]))
	remaining := uint64(len(payload) - headerSize)
	if keySize > remaining || valueSize != remaining-keySize {
		return cmd, errors.New("malformed payload")
	}
	if keySize == 0 {
		return cmd, ErrEmptyKey
	}
	cmd.key = payload[headerSize : headerSize+int(keySize)]
	cmd.value = payload[headerSize+int(keySize):]
	return cmd, nil
}

type mutationResult struct {
	success bool
	value   int64
	err     error
}

// Result is private to the engine: error code:u8 | success:u8 | value:i64.
// repl forwards it without interpreting the bytes.
func encodeMutationResult(result mutationResult) ([]byte, error) {
	errorsByCode := [...]error{nil, ErrKeyNotFound, ErrNotANumber, ErrOverflow}
	for code, known := range errorsByCode {
		if result.err == nil && known == nil || known != nil && errors.Is(result.err, known) {
			data := make([]byte, 10)
			data[0] = byte(code)
			if result.success {
				data[1] = 1
			}
			binary.BigEndian.PutUint64(data[2:], uint64(result.value))
			return data, nil
		}
	}
	return nil, result.err
}

func decodeMutationResult(data []byte) (mutationResult, error) {
	errorsByCode := [...]error{nil, ErrKeyNotFound, ErrNotANumber, ErrOverflow}
	if len(data) != 10 || int(data[0]) >= len(errorsByCode) || data[1] > 1 {
		return mutationResult{}, errors.New("invalid mutation result")
	}
	return mutationResult{success: data[1] == 1, value: int64(binary.BigEndian.Uint64(data[2:])), err: errorsByCode[data[0]]}, nil
}
