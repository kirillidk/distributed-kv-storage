// Package sharding defines the key placement rule shared by engines and clients.
package sharding

import "hash/crc32"

// Assignment identifies a replica's keyspace. Index is the position of ShardID
// among the cluster's lexicographically sorted shard names.
type Assignment struct {
	ClusterID string `json:"cluster_id"`
	ShardID   string `json:"shard_id"`
	Index     uint32 `json:"index"`
	Count     uint32 `json:"count"`
}

// ForKey returns the index into the sorted shard names for the exact key bytes.
// count must be the positive number of logical shards in the manifest.
func ForKey(key []byte, count uint32) uint32 {
	if count == 1 {
		return 0
	}
	return crc32.ChecksumIEEE(key) % count
}
