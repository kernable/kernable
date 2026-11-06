//! The account reactors: side effects and follow-up commands.

use kernable::{BoxError, Domain, Error, ListenerContext, Query, Submit};

use super::commands::{AddSessionFactor, EndSession, OpenSession};
use super::events::*;
use super::queries::ListSessions;
use super::readmodels::SessionRow;
use super::{SecondFactor, SessionEnd};
use crate::shared::constants::{PLATFORM_TENANT, orders};
use crate::shared::internal;
use crate::shared::mail::{Mail, Purpose, current_mailer};
use crate::shared::secrets::token;

fn mail(to: Option<String>, purpose: Purpose, link: String, subject: &str, text: String) {
    if let Some(to) = to.filter(|t| !t.is_empty()) {
        current_mailer().send(Mail {
            to,
            subject: subject.to_owned(),
            text,
            purpose,
            token: link,
        });
    }
}

/// Ends every open session of the account, but `keep`.
async fn end_all(
    account_id: &str,
    reason: SessionEnd,
    ctx: &ListenerContext,
    keep: Option<&str>,
) -> Result<(), BoxError> {
    let sessions: Vec<SessionRow> = ctx
        .query(
            Query::of(ListSessions {
                account_id: account_id.to_owned(),
            })
            .tenant(PLATFORM_TENANT)
            .context(internal(None)),
        )
        .await?;
    for s in sessions
        .iter()
        .filter(|s| s.active && Some(s.id.as_str()) != keep)
    {
        let submit = Submit::command(
            EndSession {
                reason: Some(reason),
            },
            &s.id,
        )
        .id(format!("end-{}-{}", s.id, reason.as_str()))
        .tenant(PLATFORM_TENANT)
        .context(internal(Some(ctx)));
        match ctx.submit(submit).await {
            // Ended meanwhile (signed out).
            Err(Error::Rejected(r)) if r.code == "invalid_state" => {}
            r => {
                r?;
            }
        }
    }
    Ok(())
}

pub fn react(domain: &Domain) {
    let mails = domain.reactor("account.mail").order(orders::REACTORS);
    mails.on(VerificationRequested, |d, evt, _| async move {
        // Erased already: nothing to send.
        if let Some(secret) = d.token {
            let link = token(&evt.aggregate_id, &d.token_id, &secret);
            mail(
                d.email,
                Purpose::Verify,
                link.clone(),
                "Confirm your email address",
                format!("Confirm your email address with this code: {link}"),
            );
        }
        Ok(())
    });
    mails.on(PasswordResetRequested, |d, evt, _| async move {
        if let Some(secret) = d.token {
            let link = token(&evt.aggregate_id, &d.token_id, &secret);
            mail(
                d.email,
                Purpose::Reset,
                link.clone(),
                "Reset your password",
                format!("Set a new password with this code: {link}"),
            );
        }
        Ok(())
    });
    mails.on(EmailChangeRequested, |d, evt, _| async move {
        if let Some(secret) = d.token {
            let link = token(&evt.aggregate_id, &d.token_id, &secret);
            mail(
                d.email,
                Purpose::EmailChange,
                link.clone(),
                "Confirm your new email address",
                format!("Confirm this address with this code: {link}"),
            );
        }
        Ok(())
    });
    mails.on(RegistrationAttempted, |d, _, _| async move {
        mail(
            d.email,
            Purpose::AccountExists,
            String::new(),
            "You already have an account",
            "Someone registered with this address, which has an account already. Sign in, or reset your password.".into(),
        );
        Ok(())
    });

    let sessions = domain.reactor("account.sessions").order(orders::REACTORS);
    // Opens the session of every sign-in.
    sessions.on(SignedIn, |d, evt, ctx| async move {
        let open = OpenSession {
            account_id: evt.aggregate_id.clone(),
            method: d.method,
            factors: d.factors,
            refresh_token_hash: d.refresh_token_hash,
            ip: d.ip,
            user_agent: d.user_agent,
            at: d.at,
        };
        let submit = Submit::command(open, &d.session_id)
            .id(format!("open-session-{}", d.session_id))
            .tenant(PLATFORM_TENANT)
            .context(internal(Some(&ctx)));
        ctx.submit(submit).await?;
        Ok(())
    });
    sessions.on(SecondFactorProved, |d, evt, ctx| async move {
        let factor = match d.method {
            SecondFactor::Totp => "totp",
            SecondFactor::Recovery => "recovery",
            SecondFactor::Passkey => "passkey",
        };
        let add = AddSessionFactor {
            factor: factor.into(),
            account_id: evt.aggregate_id.clone(),
        };
        let submit = Submit::command(add, &d.session_id)
            .id(format!("factor-{}-{}", d.session_id, evt.command_id))
            .tenant(PLATFORM_TENANT)
            .context(internal(Some(&ctx)));
        ctx.submit(submit).await?;
        Ok(())
    });

    let ending = domain.reactor("account.ending").order(orders::REACTORS);
    ending.on(AccountSuspended, |_d, evt, ctx| async move {
        end_all(&evt.aggregate_id, SessionEnd::AccountSuspended, &ctx, None).await
    });
    ending.on(AccountDeleted, |_d, evt, ctx| async move {
        end_all(&evt.aggregate_id, SessionEnd::AccountDeleted, &ctx, None).await
    });
    // A new password signs out everywhere else.
    ending.on(PasswordChanged, |d, evt, ctx| async move {
        end_all(
            &evt.aggregate_id,
            SessionEnd::PasswordChanged,
            &ctx,
            d.keep_session_id.as_deref(),
        )
        .await
    });
    ending.on(PasswordReset, |_d, evt, ctx| async move {
        end_all(&evt.aggregate_id, SessionEnd::PasswordChanged, &ctx, None).await
    });
}
