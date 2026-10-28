//! Unit tests of the tutorial's Order aggregate, without a kernel: fold events into state, then
//! call a command handler on that state. The how-to "Test your domain" shows them.
use kernable::{CommandContext, Error, FoldEvent, NewEvent, RequestContext, Timestamp};
use serde_json::{Value, json};

// #region test
/// Runs `command_type` on the state after `history`, as a worker would.
fn decide(history: &[(&str, Value)], command_type: &str) -> Result<Vec<NewEvent>, Error> {
    let order_aggregate = shop::shop().order_aggregate;
    let state = order_aggregate.fold(
        history.iter().map(|(t, data)| FoldEvent::new(t, data)),
        None,
    )?;
    let mut ctx = CommandContext::new(
        "test",
        RequestContext::default(),
        Timestamp(1_767_225_600_000),
    );
    order_aggregate.handle(&state, command_type, (), &mut ctx)
}

#[test]
fn a_draft_order_with_items_can_be_submitted() {
    let placed = ("OrderPlaced", json!({ "customerId": "c1" }));
    let added = ("ItemAdded", json!({ "sku": "apple", "qty": 2 }));
    assert_eq!(
        decide(&[placed, added], "Submit").unwrap(),
        [NewEvent::new("OrderSubmitted", ())]
    );
}

#[test]
fn an_empty_order_cannot_be_submitted() {
    let placed = ("OrderPlaced", json!({ "customerId": "c1" }));
    assert!(
        matches!(decide(&[placed], "Submit"), Err(Error::Rejected(r)) if r.code == "invalid_state")
    );
}
// #endregion test
