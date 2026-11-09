//! Scheduled commands (design/processes.md, "Timers: commands with a delivery time").
//!
//! A command with `deliver_at` after the time it arrives is checked like any other at submit,
//! stored as pending and answered `accepted` at once. It waits in the queue's timer list
//! (see `queue.rs`), not in its aggregate's FIFO. When it is due, [`run`] looks at it on the
//! kernel that serves its tenant: past `deliver_until` it is rejected `expired`; otherwise the
//! middleware chain checks it again, with the vault values the vault holds for it then, and
//! it joins its aggregate's FIFO, where the lease-time checks (a revoked key) apply as to any
//! command. A due command that cannot be checked now (its tenant moving, a middleware missing,
//! the kernel fenced) is looked at again a moment later, so a command runs late, never not.
//!
//! The timer list is not persisted: the store holds the commands as pending, with their times,
//! and the queue routes them again whenever it is rebuilt from `Store::pending` (a restart, a
//! standby's promotion) or a moving tenant arrives ([`route_tenant`]).
//!
//! [`cancel`] completes a scheduled command that still waits as rejected `cancelled`.

use std::{
    sync::{Arc, atomic::Ordering::Relaxed},
    time::Duration,
};

use kernable_core::{
    proto::{
        CancelRequest, CancelResult, Cancelled, Command, CommandResult, MiddlewareCall, Rejected,
        When, cancel_result, command_result::Outcome, middleware_call,
    },
    signing,
};
use kernable_store::Error as StoreError;
use tonic::{Request, Status};

use crate::{
    Kernel, MAX_REQUEST_AGE_MS,
    config::{self, Limits},
    fenced, internal, now_ms, protocol, queue, sealed,
};

const DAY_MS: i64 = 86_400_000;

/// How long a due command waits to be looked at again when it cannot be checked now.
const RETRY_MS: i64 = 1_000;

/// The longest the timer task sleeps without looking at the clock again.
const LONGEST_SLEEP: Duration = Duration::from_secs(60);

/// Refuses a command whose delivery times make no sense: negative, due more than
/// `--schedule-max-days` after `now`, or a `deliver_until` before its `deliver_at`.
pub(crate) fn check_times(cmd: &Command, now: i64, limits: &Limits) -> Result<(), Status> {
    if cmd.deliver_at < 0 || cmd.deliver_until < 0 {
        return Err(Status::invalid_argument(
            "deliver_at and deliver_until are ms since epoch, not negative",
        ));
    }
    let days = i64::try_from(limits.schedule_max_days).unwrap_or(i64::MAX);
    if cmd.deliver_at > now.saturating_add(days.saturating_mul(DAY_MS)) {
        return Err(Status::invalid_argument(format!(
            "deliver_at is more than {days} days ahead (--schedule-max-days)"
        )));
    }
    if cmd.deliver_until != 0 && cmd.deliver_until < cmd.deliver_at {
        return Err(Status::invalid_argument(
            "deliver_until is before deliver_at: the command could never run",
        ));
    }
    Ok(())
}

/// Looks at scheduled commands as they fall due, for as long as the kernel runs. Woken early
/// by [`Kernel::timers`] when commands arrive.
pub(crate) async fn run(kernel: &Arc<Kernel>) {
    loop {
        let woken = kernel.timers.notified();
        let now = now_ms();
        let wait = match kernel.queue.next_timer() {
            Some(at) if at <= now => Duration::ZERO,
            Some(at) => Duration::from_millis((at - now) as u64).min(LONGEST_SLEEP),
            None => LONGEST_SLEEP,
        };
        if !wait.is_zero() {
            tokio::select! {
                () = woken => {}
                () = tokio::time::sleep(wait) => {}
            }
            continue;
        }
        // Handing its role over, fenced or shutting down, it writes nothing: due commands wait
        // for the kernel that serves on.
        if sealed(kernel) || kernel.fenced() || kernel.draining.load(Relaxed) {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        for (command, _) in kernel.queue.take_due(now) {
            let kernel = Arc::clone(kernel);
            tokio::spawn(async move { due(&kernel, command).await });
        }
    }
}

/// Whether this kernel runs the commands of `tenant`.
enum Here {
    Yes,
    /// Not now: the tenant moves, or the kernel has no partition map yet.
    NotNow,
    /// Another partition serves the tenant: its copy of the command runs there.
    Elsewhere,
}

fn here(kernel: &Kernel, tenant: &str) -> Here {
    if kernel.owns(tenant).is_ok() {
        return Here::Yes;
    }
    match kernel.maps.get() {
        Some(map) if !map.moving.iter().any(|m| m.tenant == tenant) => Here::Elsewhere,
        _ => Here::NotNow,
    }
}

/// Puts a due command back, to be looked at again in a moment.
fn retry(kernel: &Kernel, command_id: &str) {
    kernel.queue.defer(command_id, now_ms() + RETRY_MS);
    kernel.timers.notify_one();
}

/// A scheduled command that fell due, taken out of the timer list: rejected `expired`, rejected
/// by a middleware, or handed to its aggregate's FIFO.
async fn due(kernel: &Arc<Kernel>, command: Command) {
    let id = command.command_id.clone();
    // Completed meanwhile (by a copy of a move, say): nothing to run.
    match kernel.store.result(&id) {
        Ok(None) => {}
        Ok(Some(_)) => {
            kernel.queue.forget(&id);
            return;
        }
        Err(e) => {
            tracing::error!(command = %id, error = %e, "reading a scheduled command failed");
            return retry(kernel, &id);
        }
    }
    match here(kernel, &command.tenant_id) {
        Here::Yes => {}
        Here::NotNow => return retry(kernel, &id),
        Here::Elsewhere => {
            kernel.queue.forget(&id);
            return;
        }
    }
    // The process engine's own (a wait's timeout, a timer): it completes them with the case.
    if command.domain == crate::process_engine::DOMAIN {
        kernel
            .engine
            .send(crate::process_engine::Input::Fired(Box::new(command)));
        return;
    }
    let now = now_ms();
    if command.deliver_until != 0 && now > command.deliver_until {
        let reason = Rejected {
            code: "expired".into(),
            message: format!(
                "the scheduled command could not run by its deliver_until; it was due at {} \
                 (ms since epoch)",
                command.deliver_at
            ),
            ..Default::default()
        };
        if let Err(e) = reject(kernel, &command, reason) {
            tracing::error!(command = %id, error = %e, "rejecting an expired command failed");
            retry(kernel, &id);
        }
        return;
    }
    // Checked again as at submit, with what the middleware knows now and the values the vault
    // still holds for it (an erased subject's are gone).
    let name = config::qualified(&command.domain, &command.command_type);
    let values = kernel.values_of(&command.vault_refs);
    let request = middleware_call::Request::Command(command.clone());
    match kernel
        .check(&name, &id, When::Before, request, &values, None)
        .await
    {
        Ok(None) => {}
        Ok(Some(reason)) => {
            if let Err(e) = reject(kernel, &command, reason) {
                tracing::error!(command = %id, error = %e, "rejecting a scheduled command failed");
                retry(kernel, &id);
            }
            return;
        }
        Err(status) => {
            tracing::warn!(command = %id, error = %status.message(), "a due command waits for its middleware");
            return retry(kernel, &id);
        }
    }
    kernel.queue.promote(&id);
    kernel.work.notify_waiters();
}

/// Completes the pending scheduled `command` as rejected for `reason` without running it, drops
/// it from the queue and tells its waiters. Returns its result: the stored one if it completed
/// otherwise meanwhile.
fn reject(
    kernel: &Arc<Kernel>,
    command: &Command,
    reason: Rejected,
) -> Result<CommandResult, StoreError> {
    let id = &command.command_id;
    let result = match kernel.store.reject(id, reason.clone()) {
        Ok(()) => {
            kernel.metrics.rejected.fetch_add(1, Relaxed);
            CommandResult {
                command_id: id.clone(),
                outcome: Some(Outcome::Rejected(reason)),
            }
        }
        Err(StoreError::NotPending) => match kernel.store.result(id)? {
            Some(result) => result,
            None => return Err(StoreError::NotPending),
        },
        Err(e) => return Err(e),
    };
    kernel.sync(0);
    kernel.drop_secrets(id);
    kernel.queue.forget(id);
    kernel.complete(command, result.clone());
    Ok(result)
}

/// Withdraws the process engine's scheduled command `command_id` if it still waits: rejected
/// `cancelled` (the awaited event came first). One due already reaches the engine, which finds
/// its step over.
pub(crate) fn withdraw(kernel: &Kernel, command_id: &str) {
    if !kernel.queue.take_waiting(command_id) {
        return;
    }
    let reason = Rejected {
        code: "cancelled".into(),
        message: "the process went on before it was due".into(),
        ..Default::default()
    };
    match kernel.store.reject(command_id, reason) {
        Ok(()) | Err(StoreError::NotPending) => {
            kernel.sync(0);
            kernel.queue.forget(command_id);
        }
        Err(e) => {
            tracing::error!(command = %command_id, error = %e, "withdrawing a process timer failed");
            kernel.queue.defer(command_id, now_ms() + RETRY_MS);
        }
    }
}

/// Routes the pending scheduled commands of `tenant`, moved here, to the timer list.
pub(crate) fn route_tenant(kernel: &Kernel, tenant: &str) -> Result<usize, StoreError> {
    let mut routed = 0;
    for p in kernel.store.pending()? {
        if p.command.tenant_id == tenant
            && queue::scheduled(&p.command, p.received_at)
            && kernel.queue.push(p.command, p.received_at)
        {
            routed += 1;
        }
    }
    if routed > 0 {
        kernel.timers.notify_one();
    }
    Ok(routed)
}

/// `CommandService.Cancel`: completes a scheduled command that still waits as rejected
/// `cancelled`.
pub(crate) async fn cancel(
    kernel: &Arc<Kernel>,
    request: Request<CancelRequest>,
) -> Result<CancelResult, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    protocol(req.protocol)?;
    kernel.owns(&req.tenant_id)?;
    let _running = kernel.request()?;
    let sig = req
        .client_sig
        .as_ref()
        .ok_or_else(|| Status::unauthenticated("cancel is not signed"))?;
    signing::verify(sig, &signing::cancel_message(&req))
        .map_err(|_| Status::unauthenticated("invalid cancel signature"))?;
    kernel.present(&metadata, &sig.key_id)?;
    let _slot = kernel.request_slot(&sig.key_id)?;
    if (now_ms() - req.issued_at).abs() > MAX_REQUEST_AGE_MS {
        return Err(Status::unauthenticated("cancel expired; check the clock"));
    }
    let id = req.command_id.clone();
    let answer = |outcome| CancelResult {
        command_id: id.clone(),
        outcome: Some(outcome),
    };
    let refused = |code: &str, message: &str| {
        answer(cancel_result::Outcome::Rejected(Rejected {
            code: code.into(),
            message: message.into(),
            ..Default::default()
        }))
    };
    let cancelled = || answer(cancel_result::Outcome::Cancelled(Cancelled {}));
    // A command of another tenant is not found either.
    let Some(stored) = kernel
        .store
        .command(&id)
        .map_err(internal)?
        .filter(|p| p.command.tenant_id == req.tenant_id)
    else {
        return Ok(refused("not_found", "no command with this ID"));
    };
    let command = stored.command;
    let name = config::qualified(&command.domain, &command.command_type);
    let permissions = kernel.permissions();
    if !config::allowed(&permissions.submit, &sig.key_id, &name) {
        return Err(Status::permission_denied(format!(
            "key may not submit {name}, so not cancel it"
        )));
    }
    let bypass = req.context.as_ref().is_some_and(|c| c.bypass_authorization);
    if bypass && !config::allowed(&permissions.privileged, &sig.key_id, "bypass_authorization") {
        return Err(Status::permission_denied(
            "key may not set bypass_authorization",
        ));
    }
    let was_cancelled = |result: &CommandResult| matches!(&result.outcome, Some(Outcome::Rejected(r)) if r.code == "cancelled");
    let too_late = || {
        refused(
            "too_late",
            "the command is due, running or completed already",
        )
    };
    if let Some(result) = kernel.store.result(&id).map_err(internal)? {
        return Ok(if was_cancelled(&result) {
            cancelled()
        } else {
            too_late()
        });
    }
    if !kernel.queue.waiting(&id) {
        return Ok(too_late());
    }
    let call = MiddlewareCall {
        request: Some(middleware_call::Request::Cancel(req.clone())),
        scheduled: Some(command.clone()),
        when: When::Before.into(),
        r#type: name.clone(),
        ..Default::default()
    };
    if let Some(reason) = kernel.check_call(&name, &id, call, &[]).await? {
        return Ok(answer(cancel_result::Outcome::Rejected(reason)));
    }
    // Out of the timer list from here: neither due nor cancelled by another request meanwhile.
    if !kernel.queue.take_waiting(&id) {
        return match kernel.store.result(&id).map_err(internal)? {
            Some(result) if was_cancelled(&result) => Ok(cancelled()),
            _ => Ok(too_late()),
        };
    }
    let reason = Rejected {
        code: "cancelled".into(),
        message: "cancelled before it was due".into(),
        ..Default::default()
    };
    let result = match reject(kernel, &command, reason) {
        Ok(result) => result,
        Err(e) => {
            kernel.queue.defer(&id, command.deliver_at);
            kernel.timers.notify_one();
            return Err(internal(e));
        }
    };
    // Told nothing while fenced: what it holds may never reach the standby.
    if kernel.fenced() {
        return Err(fenced());
    }
    Ok(if was_cancelled(&result) {
        cancelled()
    } else {
        too_late()
    })
}
