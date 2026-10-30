//! The vault: its key can change, and an erasure stays within its tenant.

use kernable_core::{proto::VaultValue, signing};
use kernable_store::vault::{Owner, Vault};

#[test]
fn rekeying_the_vault_moves_every_value_to_the_new_key() {
    let path = std::env::temp_dir().join(format!("kernable-vault-rekey-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let url = format!("sqlite:{}", path.display());
    let (old, new) = ([1u8; 32], [2u8; 32]);
    let value = b"\"max@example.com\"".to_vec();
    let salt = vec![7u8; 16];
    let personal = VaultValue {
        r#ref: signing::vault_ref(&salt, "k-1", false, &value),
        subject: "k-1".into(),
        secret: false,
        salt,
        value: value.clone(),
    };
    let refs = vec![personal.r#ref.clone()];
    let owner = Owner {
        command_id: "c1",
        tenant: "t1",
        stream: None,
    };
    Vault::open(&url, &old)
        .unwrap()
        .put(&[personal], &refs, &owner, 0)
        .unwrap();

    assert_eq!(Vault::open(&url, &old).unwrap().rekey(&new).unwrap(), 1);
    assert_eq!(
        Vault::open(&url, &new).unwrap().get(&refs).unwrap()[0].value,
        value
    );
    assert!(
        Vault::open(&url, &old).unwrap().get(&refs).is_err(),
        "the old key no longer reads it"
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn an_erasure_deletes_the_subject_of_its_own_tenant_only() {
    let vault = Vault::open("memory:", &[3u8; 32]).unwrap();
    let value = |tenant: &str| {
        let salt = tenant.as_bytes().to_vec();
        let value = format!("\"max@{tenant}.example\"").into_bytes();
        VaultValue {
            r#ref: signing::vault_ref(&salt, "k-1", false, &value),
            subject: "k-1".into(),
            secret: false,
            salt,
            value,
        }
    };
    let (a, b) = (value("a"), value("b"));
    for (tenant, v) in [("a", &a), ("b", &b)] {
        let owner = Owner {
            command_id: tenant,
            tenant,
            stream: None,
        };
        vault
            .put(
                std::slice::from_ref(v),
                std::slice::from_ref(&v.r#ref),
                &owner,
                0,
            )
            .unwrap();
    }

    vault.erase("a", "k-1", "erase-a", 1).unwrap();
    assert!(
        vault
            .get(std::slice::from_ref(&a.r#ref))
            .unwrap()
            .is_empty(),
        "erased in tenant a"
    );
    assert_eq!(
        vault.get(std::slice::from_ref(&b.r#ref)).unwrap().len(),
        1,
        "tenant b's k-1 keeps its value"
    );
}

#[test]
fn describe_tells_what_the_vault_holds_without_the_values() {
    let vault = Vault::open("memory:", &[4u8; 32]).unwrap();
    let salt = vec![5u8; 16];
    let email = b"\"max@example.com\"".to_vec();
    let token = b"\"123456\"".to_vec();
    let personal = VaultValue {
        r#ref: signing::vault_ref(&salt, "k-1", false, &email),
        subject: "k-1".into(),
        secret: false,
        salt: salt.clone(),
        value: email,
    };
    let secret = VaultValue {
        r#ref: signing::vault_ref(&salt, "", true, &token),
        subject: String::new(),
        secret: true,
        salt,
        value: token,
    };
    let refs = vec![personal.r#ref.clone(), secret.r#ref.clone()];
    let owner = Owner {
        command_id: "c1",
        tenant: "t1",
        stream: None,
    };
    vault
        .put(&[personal.clone(), secret.clone()], &refs, &owner, 0)
        .unwrap();

    let mut asked = refs.clone();
    asked.push("unknown".into());
    let mut described = vault.describe(&asked).unwrap();
    described.sort();
    let mut expected = vec![
        (personal.r#ref.clone(), "k-1".to_owned(), false),
        (secret.r#ref.clone(), String::new(), true),
    ];
    expected.sort();
    assert_eq!(described, expected);

    vault.erase("t1", "k-1", "c2", 0).unwrap();
    assert_eq!(
        vault.describe(&refs).unwrap(),
        [(secret.r#ref.clone(), String::new(), true)],
        "an erased value is no longer held"
    );
}
