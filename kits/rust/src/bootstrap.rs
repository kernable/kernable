//! The bootstrap: run once, with a key granted `privileged` for `bypass_authorization`. It
//! creates the platform tenant, the sign-up policy, and the API's service identity with its key.

use kernable::{CommandType, Kernable, Outcome, RequestContext, Submit};

use crate::account::RegisterAccount;
use crate::group::{CreateGroup, Permission};
use crate::identity::{
    CreateApiKey, CreateIdentity, CreateServiceIdentity, GrantMembership, api_key,
};
use crate::shared::constants::PLATFORM_TENANT;
use crate::shared::secrets::{random_bytes, random_secret};
use crate::tenancy::{AddOwner, ChangeSignUpPolicy, CreateTenant, Owner, SignUpPolicy};

/// What the API may ask for itself: lookups before anyone is signed in.
pub const API_PERMISSIONS: &[&str] = &[
    "account.FindAccountByEmail",
    "account.GetAccount",
    "account.FindAccountByLink",
    "account.RegisterAccount",
    "account.RequestVerification",
    "account.RequestPasswordReset",
    "account.NoteRegistrationAttempt",
    "account.SignInWithPasskey",
    "account.ProvePasskey",
    "account.SignInWithProvider",
    "account.LinkProvider",
    "account.VerifySsoDomain",
    "account.FindSsoByDomain",
    "account.GetSsoSecret",
    "account.SetCache",
    "account.GetCache",
    "account.DeleteCache",
    "identity.FindIdentityByAccount",
    "identity.FindApiKey",
    "tenancy.GetTenantBySlug",
    "tenancy.GetSignUpPolicy",
    "invitation.AcceptInvitation",
    "invitation.PeekInvitation",
    "invitation.JoinWithInvitationLink",
    "invitation.PeekInvitationLink",
];

/// The first platform administrator: an owner of the platform tenant.
pub struct Admin<'a> {
    pub email: &'a str,
    pub name: &'a str,
    pub password: &'a str,
}

/// What the bootstrap made.
pub struct Bootstrapped {
    /// The API's identity, its key ID and key: give them to `KitApi`, keep the key secret.
    pub api_identity: String,
    pub api_key_id: String,
    pub api_key: String,
    /// The administrator's account and identity, if asked for.
    pub admin: Option<(String, String)>,
}

async fn ok<C: CommandType>(
    kernable: &Kernable,
    command: C,
    aggregate_id: &str,
    id: Option<String>,
) -> Result<(), String> {
    let id = id.unwrap_or_else(|| format!("bootstrap-{}-{aggregate_id}", C::TYPE));
    let submit = Submit::command(command, aggregate_id)
        .id(id)
        .tenant(PLATFORM_TENANT)
        .context(RequestContext {
            bypass_authorization: true,
            ..RequestContext::default()
        });
    match kernable
        .submit(submit)
        .await
        .map_err(|e| format!("bootstrap: {e}"))?
    {
        Outcome::Rejected(r) if r.code != "exists" => {
            Err(format!("bootstrap: {}: {}", r.code, r.message))
        }
        _ => Ok(()),
    }
}

/// Sets up the platform: idempotent, so it may run again.
pub async fn bootstrap(
    kernable: &Kernable,
    sign_up: SignUpPolicy,
    platform_name: &str,
    admin: Option<Admin<'_>>,
) -> Result<Bootstrapped, String> {
    let tenant = PLATFORM_TENANT;
    let owner = Owner {
        email: Some(String::new()),
        account_id: None,
    };
    ok(
        kernable,
        CreateTenant {
            slug: "platform".into(),
            name: platform_name.into(),
            owner,
            status: None,
        },
        tenant,
        None,
    )
    .await?;
    ok(
        kernable,
        ChangeSignUpPolicy {
            tenants: sign_up.tenants,
            accounts: sign_up.accounts,
        },
        tenant,
        None,
    )
    .await?;
    let identity = "api";
    ok(
        kernable,
        CreateServiceIdentity {
            display_name: "API".into(),
        },
        identity,
        None,
    )
    .await?;
    let permissions = API_PERMISSIONS
        .iter()
        .map(|p| Permission {
            pattern: (*p).into(),
            ..Permission::default()
        })
        .collect();
    let group = CreateGroup {
        name: "api".into(),
        description: Some("What the API asks for itself".into()),
        permissions,
    };
    ok(kernable, group, "api", None).await?;
    ok(
        kernable,
        GrantMembership {
            membership_id: "api".into(),
            group: "api".into(),
            scope: None,
            expires_at: None,
        },
        identity,
        None,
    )
    .await?;
    let key_id = data_encoding::HEXLOWER.encode(&random_bytes(8));
    let secret = random_secret(32);
    let key = CreateApiKey {
        key_id: key_id.clone(),
        secret: secret.clone(),
        label: Some("API".into()),
        expires_at: None,
    };
    ok(
        kernable,
        key,
        identity,
        Some(data_encoding::HEXLOWER.encode(&random_bytes(8))),
    )
    .await?;
    let mut first = None;
    if let Some(a) = admin {
        // Fixed IDs, so a second run finds them: the account (verified), its platform identity, owner.
        let (account, identity_id) = ("platform-admin", "platform-admin");
        let register = RegisterAccount {
            email: a.email.into(),
            name: a.name.into(),
            password: Some(a.password.into()),
            verified: Some(true),
        };
        ok(kernable, register, account, None).await?;
        let create = CreateIdentity {
            account_id: account.into(),
            display_name: Some(a.name.into()),
            title: None,
            email: None,
        };
        ok(kernable, create, identity_id, None).await?;
        ok(
            kernable,
            AddOwner {
                identity_id: identity_id.into(),
            },
            tenant,
            Some("bootstrap-admin-owner".into()),
        )
        .await?;
        first = Some((account.to_owned(), identity_id.to_owned()));
    }
    Ok(Bootstrapped {
        api_identity: identity.into(),
        api_key: api_key(&key_id, &secret),
        api_key_id: key_id,
        admin: first,
    })
}
