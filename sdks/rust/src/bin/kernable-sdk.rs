//! kernable-sdk: the SDK's own tool. `kernable-sdk add kit [dir]` copies the kit of this SDK's
//! version into your project: tenancy, accounts, identities, groups, invitations and
//! authorization as source that is yours to change. Install it with `cargo install kernable-sdk`.
//!
//! The copy is a crate named after `dir` (default `kit`): add it to your workspace and depend on it.

use std::path::Path;
use std::process::ExitCode;

include!(concat!(env!("OUT_DIR"), "/kit_files.rs"));

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// `kit::` in the copy's tests and example, renamed to the copy's library name.
fn rename(text: &str, lib: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(i) = rest.find("kit::") {
        let before = rest[..i].chars().last();
        out.push_str(&rest[..i]);
        if before.is_some_and(|c| c.is_alphanumeric() || c == '_') {
            out.push_str("kit::");
        } else {
            out.push_str(lib);
            out.push_str("::");
        }
        rest = &rest[i + 5..];
    }
    out.push_str(rest);
    out
}

/// The kit's manifest for the copy: its own name, the SDK from crates.io at this version, and
/// what it took from kernable's workspace written out (the copy's project has no such workspace).
fn manifest(text: &str, package: &str, lib: &str) -> String {
    text.lines()
        .map(|line| {
            if line.starts_with("license.workspace") {
                "license = \"MIT\"".to_owned()
            } else if line.starts_with("name = \"kernable-kit\"") {
                format!("name = {package:?}")
            } else if line.starts_with("name = \"kit\"") {
                format!("name = {lib:?}")
            } else if line.starts_with("kernable-sdk = ") {
                format!("kernable-sdk = \"{VERSION}\"")
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n"
}

fn add_kit(dir: &str, force: bool) -> ExitCode {
    if !FILES.iter().any(|(name, _)| name.starts_with("src/")) {
        eprintln!("kernable-sdk: this SDK carries no kit");
        return ExitCode::FAILURE;
    }
    let target = Path::new(dir);
    let package = target
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("kit")
        .to_owned();
    let valid = package.starts_with(|c: char| c.is_ascii_alphabetic())
        && package
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if !valid {
        eprintln!(
            "kernable-sdk: {dir} names the copied crate: letters, digits, - and _ (kit, auth-kit)"
        );
        return ExitCode::FAILURE;
    }
    let lib = package.replace('-', "_");
    let occupied = target
        .read_dir()
        .map(|mut d| d.next().is_some())
        .unwrap_or(false);
    if occupied && !force {
        eprintln!(
            "kernable-sdk: {dir} is not empty (the kit is copied, not merged); choose another directory, or --force"
        );
        return ExitCode::FAILURE;
    }
    let header =
        format!("// From the kernable kit {VERSION} (MIT, see LICENSE): yours to change.\n");
    for (name, bytes) in FILES {
        let path = target.join(name);
        std::fs::create_dir_all(path.parent().unwrap()).expect("the directory can be created");
        let content: Vec<u8> = if *name == "Cargo.toml" {
            manifest(&String::from_utf8_lossy(bytes), &package, &lib).into_bytes()
        } else if name.ends_with(".rs") {
            let text = String::from_utf8_lossy(bytes);
            let text = if name.starts_with("src/") {
                text.into_owned()
            } else {
                rename(&text, &lib)
            };
            if text.starts_with("// From the kernable kit") {
                text
            } else {
                header.clone() + &text
            }
            .into_bytes()
        } else {
            bytes.to_vec()
        };
        std::fs::write(&path, content).expect("the file can be written");
    }
    println!(
        "The kernable kit {VERSION} is in {dir} ({} files), yours to change.\n\
         Add it to your workspace and depend on it:  {package} = {{ path = \"{dir}\" }}\n\
         Its contract:  cargo test -p {package}\n\
         Then see {}.",
        FILES.len(),
        target.join("README.md").display()
    );
    ExitCode::SUCCESS
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let force = args.iter().any(|a| a == "--force");
    let rest: Vec<&str> = args
        .iter()
        .map(String::as_str)
        .filter(|a| *a != "--force")
        .collect();
    match rest.as_slice() {
        ["add", "kit"] => add_kit("kit", force),
        ["add", "kit", dir] => add_kit(dir, force),
        ["--version"] => {
            println!("{VERSION}");
            ExitCode::SUCCESS
        }
        [] | ["--help"] => {
            eprintln!(
                "usage: kernable-sdk add kit [dir] [--force]   copies the kit into dir (default: kit)"
            );
            ExitCode::SUCCESS
        }
        _ => {
            eprintln!(
                "usage: kernable-sdk add kit [dir] [--force]   copies the kit into dir (default: kit)"
            );
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_copied_manifest_stands_alone() {
        let kit = include_str!("../../../../kits/rust/Cargo.toml");
        let copy = manifest(kit, "kit", "kit");
        assert!(!copy.contains(".workspace"), "{copy}");
        assert!(copy.contains("license = \"MIT\""));
        assert!(copy.contains(&format!("kernable-sdk = \"{VERSION}\"")));
    }
}
