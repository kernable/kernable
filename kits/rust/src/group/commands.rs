//! The group commands: each type with its handler.

use kernable::{Aggregate, Rejected, command};

use super::{Group, Permission, events::*};

fn rejected(message: &str, code: &str) -> Rejected {
    Rejected::new(message).code(code)
}

fn word(value: &str, first: impl Fn(char) -> bool) -> bool {
    value.starts_with(first)
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// `*`, `domain.*` or `domain.Type`.
pub fn valid_pattern(p: &str) -> bool {
    if p == "*" {
        return true;
    }
    let Some((domain, rest)) = p.split_once('.') else {
        return false;
    };
    let name_ok = rest == "*" || (word(rest, |c| c.is_ascii_alphabetic()) && !rest.contains('-'));
    word(domain, |c| c.is_ascii_lowercase()) && name_ok
}

/// The permissions, checked and normalised.
pub fn permissions(value: Vec<Permission>) -> Result<Vec<Permission>, Rejected> {
    value
        .into_iter()
        .map(|p| {
            if !valid_pattern(&p.pattern) {
                return Err(rejected(
                    &format!("Not a type pattern: {}", p.pattern),
                    "invalid_permissions",
                ));
            }
            if let Some(when) = &p.when
                && !(word(when, |c| c.is_ascii_lowercase()) && !when.contains('-'))
            {
                return Err(rejected(
                    &format!("Not a relation: {when}"),
                    "invalid_permissions",
                ));
            }
            let when = p.when.filter(|w| !w.is_empty());
            let via_parent = when.as_ref().and(p.via_parent.filter(|v| *v));
            Ok(Permission {
                pattern: p.pattern,
                when,
                via_parent,
            })
        })
        .collect()
}

fn name(value: &str) -> Result<String, Rejected> {
    let trimmed = value.trim();
    if trimmed.is_empty() || value.chars().count() > 100 || trimmed.eq_ignore_ascii_case("owners") {
        return Err(rejected(
            "A group needs a name other than owners",
            "invalid_name",
        ));
    }
    Ok(trimmed.to_owned())
}

/// The middleware lets the sender hand out only permissions it holds.
#[command("group", aggregate = "group")]
pub struct CreateGroup {
    pub name: String,
    pub description: Option<String>,
    pub permissions: Vec<Permission>,
}

#[command("group", aggregate = "group")]
pub struct UpdateGroup {
    pub name: Option<String>,
    pub description: Option<String>,
}

#[command("group", aggregate = "group")]
pub struct SetPermissions {
    pub permissions: Vec<Permission>,
}

/// Its memberships stop counting at once (the middleware); a reactor removes them later.
#[command("group", aggregate = "group")]
pub struct DeleteGroup {}

fn active(s: &Group) -> Result<(), Rejected> {
    if s.active {
        Ok(())
    } else {
        Err(rejected("No such group", "not_found"))
    }
}

pub fn handle(group: &Aggregate<Group>) {
    group.command(CreateGroup, |s, d, _, _| {
        if s.active || s.deleted {
            return Err(rejected("The group exists", "exists"));
        }
        Ok(vec![
            GroupCreated {
                name: name(&d.name)?,
                description: d.description.unwrap_or_default(),
                permissions: permissions(d.permissions)?,
            }
            .into(),
        ])
    });
    group.command(UpdateGroup, |s, d, _, _| {
        active(s)?;
        Ok(vec![
            GroupUpdated {
                name: d.name.as_deref().map(name).transpose()?,
                description: d.description,
            }
            .into(),
        ])
    });
    group.command(SetPermissions, |s, d, _, _| {
        active(s)?;
        Ok(vec![
            GroupPermissionsSet {
                permissions: permissions(d.permissions)?,
            }
            .into(),
        ])
    });
    group.command(DeleteGroup, |s, _, _, _| {
        active(s)?;
        Ok(vec![GroupDeleted {}.into()])
    });
}
