package idempotency

import "testing"

func TestHashBody_SameInputSameHash(t *testing.T) {
	a := HashBody([]byte(`{"customer_id":"c1"}`))
	b := HashBody([]byte(`{"customer_id":"c1"}`))
	if a != b {
		t.Fatalf("expected identical hashes for identical bodies, got %s vs %s", a, b)
	}
}

func TestHashBody_DifferentInputDifferentHash(t *testing.T) {
	a := HashBody([]byte(`{"customer_id":"c1"}`))
	b := HashBody([]byte(`{"customer_id":"c2"}`))
	if a == b {
		t.Fatalf("expected different hashes for different bodies")
	}
}
