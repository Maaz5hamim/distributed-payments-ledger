package shard

import (
	"fmt"
	"testing"
)

func TestStableAndDistributed(t *testing.T) {
	counts := make([]int, 4)
	for i := 0; i < 10000; i++ {
		id := fmt.Sprint(i)
		r := Route(id, 4)
		if r != Route(id, 4) {
			t.Fatal("unstable routing")
		}
		counts[r]++
		if n := Route(id, 5); n != r && n != 4 {
			t.Fatal("adding a shard moved ownership between existing shards")
		}
	}
	for _, n := range counts {
		if n < 2200 || n > 2800 {
			t.Fatal(counts)
		}
	}
}
func TestEmptyMap(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	Route("x", 0)
}
