package shard

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// Rendezvous hashing uses stable shard indices. Never reorder a deployed map.
// Adding a shard changes ownership and requires migrating affected accounts.
func Route(id string, n int) int {
	if n < 1 {
		panic("empty shard map")
	}
	best := uint64(0)
	index := 0
	for i := 0; i < n; i++ {
		sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", id, i)))
		score := binary.BigEndian.Uint64(sum[:8])
		if i == 0 || score > best {
			best = score
			index = i
		}
	}
	return index
}
