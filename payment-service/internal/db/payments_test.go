package db

import "testing"

func TestSimulateAuthorization_Deterministic(t *testing.T) {
	orderID := "11111111-1111-1111-1111-111111111111"
	first := simulateAuthorization(orderID)
	second := simulateAuthorization(orderID)
	if first != second {
		t.Fatalf("expected deterministic result for same order id, got %v then %v", first, second)
	}
}
