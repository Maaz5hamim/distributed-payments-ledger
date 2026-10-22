package ledger

import "testing"

func TestIDs(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := ID()
		if !ValidID(id) || seen[id] {
			t.Fatalf("invalid or duplicate ID: %s", id)
		}
		seen[id] = true
	}
}
func TestInvalidIDs(t *testing.T) {
	for _, s := range []string{"", "abc", "000000000000000000000000000000000000", "zzzzzzzz-0000-0000-0000-000000000000"} {
		if ValidID(s) {
			t.Fatalf("accepted %q", s)
		}
	}
}
