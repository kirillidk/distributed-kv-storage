// Package sharding defines the key placement rule shared by engines and clients.
package sharding

import "hash/crc32"

// ForKey returns the zero-based shard ID for the exact bytes of key.
// count must be the positive shard_count from the cluster manifest.
func ForKey(key []byte, count uint32) uint32 {
	if count == 1 {
		return 0
	}
	return crc32.ChecksumIEEE(key) % count
}
