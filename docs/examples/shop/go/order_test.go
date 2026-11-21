// Unit tests of the tutorial's Order aggregate, without a kernel: fold events into state, then
// call a command handler on that state. The how-to "Test your domain" shows them.
package shop

import (
	"errors"
	"testing"
	"time"

	kernable "github.com/kernable/kernable/sdks/go"
)

// #region test
// decide runs commandType on the state after history, as a worker would.
func decide(t *testing.T, history []kernable.FoldEvent, commandType string) ([]kernable.NewEvent, error) {
	t.Helper()
	state, err := OrderAggregate.Fold(history)
	if err != nil {
		t.Fatal(err)
	}
	ctx := kernable.NewCommandContext(t.Context(), "test", kernable.RequestContext{}, time.UnixMilli(1_767_225_600_000))
	return OrderAggregate.Handle(ctx, state, commandType, nil)
}

func event(eventType string, data any) kernable.FoldEvent {
	return kernable.FoldEvent{EventMeta: kernable.EventMeta{Type: eventType}, Schema: 1, Data: data}
}

func TestADraftOrderWithItemsCanBeSubmitted(t *testing.T) {
	placed := event("OrderPlaced", map[string]any{"customerId": "c1"})
	added := event("ItemAdded", map[string]any{"sku": "apple", "qty": 2})
	events, err := decide(t, []kernable.FoldEvent{placed, added}, "Submit")
	if err != nil || len(events) != 1 || events[0].Type != "OrderSubmitted" {
		t.Fatalf("%v %v", events, err)
	}
}

func TestAnEmptyOrderCannotBeSubmitted(t *testing.T) {
	placed := event("OrderPlaced", map[string]any{"customerId": "c1"})
	_, err := decide(t, []kernable.FoldEvent{placed}, "Submit")
	var r *kernable.Rejected
	if !errors.As(err, &r) || r.Code != "invalid_state" {
		t.Fatalf("not rejected invalid_state: %v", err)
	}
}

// #endregion test
