//! Scheduled commands: enqueued with `at`, run at their time while the aggregate's other commands
//! pass; cancelled before they are due; a middleware sees the cancel as the command's type.
//! Ported from the TypeScript SDK's tests.

mod common;

use std::{
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use common::TestKernel;
use kernable::{
    Cancel, CancelOutcome, CheckKind, Error, MiddlewareOptions, NewEvent, Outcome, Rejected,
    RequestContext, Submit, domain,
};
use serde_json::Value;

#[tokio::test(flavor = "multi_thread")]
async fn a_scheduled_command_runs_at_its_time_others_pass_meanwhile_and_a_cancel_stops_one() {
    let kernel = TestKernel::new(&["shop.*"], &[]);
    kernel.start().await;
    let kernable = kernel.client();
    let shop = domain("shop");
    let ran = Arc::new(Mutex::new(Vec::<(String, SystemTime)>::new()));
    let order = shop.aggregate("order", 0u32);
    let r = Arc::clone(&ran);
    order.command("Remind", move |_s, _: Value, cmd, _ctx| {
        r.lock()
            .unwrap()
            .push((cmd.command_id.clone(), SystemTime::now()));
        Ok(vec![NewEvent::new("Reminded", ())])
    });
    order.command("Note", |_s, _: Value, _cmd, _ctx| {
        Ok(vec![NewEvent::new("Noted", ())])
    });
    let cancels = Arc::new(Mutex::new(Vec::<String>::new()));
    let c = Arc::clone(&cancels);
    let guard = shop.read_model("guard");
    guard.middleware(MiddlewareOptions::new(&["shop.*"]), move |check| {
        let refused = check.kind == CheckKind::Cancel && {
            c.lock().unwrap().push(format!(
                "{} {} {} {}",
                check.type_name,
                check.aggregate_type.clone().unwrap_or_default(),
                check.aggregate_id.clone().unwrap_or_default(),
                check
                    .target
                    .as_ref()
                    .map(|t| t.aggregate_type.as_str())
                    .unwrap_or_default(),
            ));
            check.request.sender_identity == "mallory"
        };
        async move {
            if refused {
                return Err(Rejected::new("Not yours").code("forbidden").into());
            }
            Ok(())
        }
    });
    let worker = kernable.worker(&order);
    let guard = kernable.listener(&guard);
    let running = {
        let (w, g) = (worker.clone(), guard.clone());
        [
            tokio::spawn(async move { w.run().await }),
            tokio::spawn(async move { g.run().await }),
        ]
    };
    worker.ready().await;
    guard.ready().await;

    let at = SystemTime::now() + Duration::from_secs(2);
    let remind = |id: &str| Submit::new("shop", "order", id, "Remind");
    let id = kernable.enqueue(remind("o1").at(at)).await.unwrap();
    // The aggregate is not blocked by it.
    assert!(
        kernable
            .submit(Submit::new("shop", "order", "o1", "Note"))
            .await
            .unwrap()
            .is_committed()
    );
    assert!(ran.lock().unwrap().is_empty());
    let submitted = kernable.submit(remind("o1").at(at)).await;
    assert!(
        matches!(&submitted, Err(Error::Config(m)) if m.contains("enqueue")),
        "{submitted:?}"
    );

    let second = kernable
        .enqueue(remind("o2").at(SystemTime::now() + Duration::from_secs(60)))
        .await
        .unwrap();
    let mallory = RequestContext {
        sender_identity: "mallory".into(),
        ..RequestContext::default()
    };
    assert_eq!(
        kernable
            .cancel(Cancel::new(&second).context(mallory))
            .await
            .unwrap(),
        CancelOutcome::Rejected(Rejected::new("Not yours").code("forbidden"))
    );
    assert_eq!(
        kernable.cancel(Cancel::new(&second)).await.unwrap(),
        CancelOutcome::Cancelled
    );
    assert_eq!(
        kernable.cancel(Cancel::new(&second)).await.unwrap(),
        CancelOutcome::Cancelled,
        "cancelling twice answers the same"
    );
    assert_eq!(
        *cancels.lock().unwrap(),
        ["Remind order o2 shop.order", "Remind order o2 shop.order"]
    );
    match kernable.submit(remind("o2").id(&second)).await.unwrap() {
        Outcome::Rejected(r) => assert_eq!(r.code, "cancelled"),
        other => panic!("expected cancelled, got {other:?}"),
    }
    assert!(
        !kernable
            .cancel(Cancel::new("nope"))
            .await
            .unwrap()
            .is_cancelled()
    );

    // The first one runs at its time, not before. Asking for its result by its ID waits for it.
    let result = kernable.submit(remind("o1").id(&id)).await.unwrap();
    assert!(result.is_committed(), "{result:?}");
    {
        let ran = ran.lock().unwrap();
        assert_eq!(ran.len(), 1);
        assert_eq!(ran[0].0, id);
        // The kernel's clock is this machine's: not before its time (to the millisecond).
        assert!(
            ran[0].1 + Duration::from_millis(1) >= at,
            "not before its time"
        );
    }
    assert!(
        !kernable
            .cancel(Cancel::new(&id))
            .await
            .unwrap()
            .is_cancelled(),
        "too late once it ran"
    );

    worker.stop();
    guard.stop();
    for r in running {
        r.await.unwrap();
    }
}
