//! The vault's key can change: every value is encrypted again with the new key.

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
