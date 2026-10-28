//! A private CA and certificates for mTLS between kernel, clients, workers and listeners.
//! For development and tests; production deployments bring their own PKI.

use std::{fs, io, path::Path};

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Writes `ca.tls.crt`, `kernel.tls.crt`/`kernel.tls.key` (valid for `localhost` and
/// `127.0.0.1`) and `<name>.tls.crt`/`<name>.tls.key` for each client name into `dir`. The
/// `.tls.` infix keeps them apart from the Ed25519 signing keys (`<name>.key`). The CA key is
/// not kept: issue a new set to add clients.
pub fn create(dir: &Path, clients: &[&str]) -> Result<(), Error> {
    fs::create_dir_all(dir)?;
    let ca_key = KeyPair::generate()?;
    let mut ca = CertificateParams::new(Vec::<String>::new())?;
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca.distinguished_name
        .push(DnType::CommonName, "kernable development CA");
    fs::write(dir.join("ca.tls.crt"), ca.self_signed(&ca_key)?.pem())?;
    let issuer = Issuer::new(ca, ca_key);

    let issue = |name: &str, sans: Vec<String>, usage: ExtendedKeyUsagePurpose| {
        let key = KeyPair::generate()?;
        let mut params = CertificateParams::new(sans)?;
        params.distinguished_name.push(DnType::CommonName, name);
        params.extended_key_usages = vec![usage];
        let cert = params.signed_by(&key, &issuer)?;
        fs::write(dir.join(format!("{name}.tls.crt")), cert.pem())?;
        write_secret(&dir.join(format!("{name}.tls.key")), &key.serialize_pem())?;
        Ok::<_, Error>(())
    };
    let local = vec!["localhost".to_owned(), "127.0.0.1".to_owned()];
    issue("kernel", local, ExtendedKeyUsagePurpose::ServerAuth)?;
    for name in clients {
        issue(name, Vec::new(), ExtendedKeyUsagePurpose::ClientAuth)?;
    }
    Ok(())
}

fn write_secret(path: &Path, content: &str) -> io::Result<()> {
    use std::io::Write;
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    // Readable by the owner only; on other systems the directory's permissions apply.
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    options.open(path)?.write_all(content.as_bytes())
}
