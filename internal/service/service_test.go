package service

import (
	"testing"
	"time"
)

func TestBackoffBounded(t *testing.T) {
	last := time.Duration(0)
	for i := 1; i < 100; i++ {
		v := Backoff(i)
		if v < last || v > 6400*time.Millisecond {
			t.Fatal(v)
		}
		last = v
	}
}
func TestTerminalStates(t *testing.T) {
	for _, state := range []string{"COMPLETED", "FAILED", "REVERSED"} {
		if !Terminal(state) {
			t.Fatal(state)
		}
	}
	for _, state := range []string{"PENDING", "DEBITED", "CANCELLING", "COMPENSATING", "COMPLETING", "REVERSING"} {
		if Terminal(state) {
			t.Fatal(state)
		}
	}
}
func TestIllegalTransitions(t *testing.T) {
	for _, pair := range [][2]string{{"DEBITED", "FAILED"}, {"DEBITED", "COMPENSATING"}, {"COMPLETED", "PENDING"}, {"REVERSED", "COMPLETED"}, {"COMPENSATING", "COMPLETED"}, {"unknown", "unknown"}} {
		if AllowedTransition(pair[0], pair[1]) {
			t.Fatal(pair)
		}
	}
	for _, pair := range [][2]string{{"PENDING", "DEBITED"}, {"PENDING", "FAILED"}, {"DEBITED", "CANCELLING"}, {"CANCELLING", "COMPENSATING"}, {"CANCELLING", "COMPLETING"}, {"COMPENSATING", "REVERSING"}, {"REVERSING", "REVERSED"}, {"COMPLETING", "COMPLETED"}, {"DEBITED", "DEBITED"}} {
		if !AllowedTransition(pair[0], pair[1]) {
			t.Fatal(pair)
		}
	}
}
