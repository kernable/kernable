//! Every command of the catalogue (design/kits.md) is registered on its aggregate: a handler that
//! its domain's module does not register would silently not exist. The SDK does not list a read
//! model's queries, so their registration is checked end to end (tests/e2e.rs) only.

use kernable::{Aggregate, CommandContext, Error, RequestContext, State, Timestamp};
use serde_json::json;

/// The commands of `names` that `aggregate` has no handler for.
fn missing<S: State>(aggregate: &Aggregate<S>, names: &[&str]) -> Vec<String> {
    names
        .iter()
        .filter(|name| {
            let mut ctx =
                CommandContext::new("registered", RequestContext::default(), Timestamp(1))
                    .on("x", "t");
            matches!(
                aggregate.handle(aggregate.initial(), name, json!({}), &mut ctx),
                Err(Error::Config(m)) if m.starts_with("no handler for ")
            )
        })
        .map(|name| format!("{}.{}", aggregate.name(), name))
        .collect()
}

#[test]
fn every_command_of_the_catalogue_is_registered_on_its_aggregate() {
    let tenancy = kit::tenancy::tenancy();
    let account = kit::account::account();
    let identity = kit::identity::identity();
    let group = kit::group::group();
    let invitation = kit::invitation::invitation();
    let mut not_registered = Vec::new();
    not_registered.extend(missing(
        &tenancy.tenant,
        &[
            "CreateTenant",
            "ActivateTenant",
            "RenameTenant",
            "ChangeTenantSlug",
            "SuspendTenant",
            "ReactivateTenant",
            "DeleteTenant",
            "AddOwner",
            "RemoveOwner",
            "RequireMfa",
            "ChangeSignUpPolicy",
        ],
    ));
    not_registered.extend(missing(
        &account.account,
        &[
            "RegisterAccount",
            "RequestVerification",
            "VerifyEmail",
            "SignIn",
            "SuspendAccount",
            "ReactivateAccount",
            "DeleteAccount",
            "ChangePassword",
            "RequestPasswordReset",
            "ResetPassword",
            "RequestEmailChange",
            "ConfirmEmailChange",
            "NoteRegistrationAttempt",
            "StartTotp",
            "ConfirmTotp",
            "DisableTotp",
            "RegenerateRecoveryCodes",
            "ProveSecondFactor",
            "AddPasskey",
            "RemovePasskey",
            "SignInWithPasskey",
            "ProvePasskey",
            "SignInWithProvider",
            "LinkProvider",
            "UnlinkProvider",
        ],
    ));
    not_registered.extend(missing(
        &account.sso,
        &[
            "CreateSso",
            "UpdateSso",
            "AddSsoDomain",
            "VerifySsoDomain",
            "RemoveSsoDomain",
            "EnforceSso",
            "DisableSso",
            "EnableSso",
            "DeleteSso",
        ],
    ));
    not_registered.extend(missing(
        &account.session,
        &[
            "OpenSession",
            "RefreshSession",
            "AddSessionFactor",
            "EndSession",
        ],
    ));
    not_registered.extend(missing(
        &identity.identity,
        &[
            "CreateIdentity",
            "CreateServiceIdentity",
            "CreateProcessIdentity",
            "UpdateProfile",
            "GrantMembership",
            "SuspendMembership",
            "ResumeMembership",
            "RevokeMembership",
            "CreateApiKey",
            "RevokeApiKey",
            "SuspendIdentity",
            "ReactivateIdentity",
            "DeleteIdentity",
        ],
    ));
    not_registered.extend(missing(
        &group.group,
        &[
            "CreateGroup",
            "UpdateGroup",
            "SetPermissions",
            "DeleteGroup",
        ],
    ));
    not_registered.extend(missing(
        &invitation.invitation,
        &[
            "CreateInvitation",
            "ResendInvitation",
            "RevokeInvitation",
            "DeclineInvitation",
            "AcceptInvitation",
            "RecordInvitationMailSent",
            "EraseInvitation",
        ],
    ));
    not_registered.extend(missing(
        &invitation.link,
        &[
            "CreateInvitationLink",
            "RevokeInvitationLink",
            "JoinWithInvitationLink",
        ],
    ));
    assert!(
        not_registered.is_empty(),
        "not registered: {not_registered:?}"
    );
    // And the check itself works: a command nobody registered is found missing.
    assert_eq!(
        missing(&group.group, &["NoSuchCommand"]),
        ["group.NoSuchCommand"]
    );
}
