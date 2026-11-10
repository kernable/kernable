//! Embeds the kit into the `kernable-sdk` binary, so `kernable-sdk add kit` copies the
//! kit of exactly this SDK's version: from `kit/` in a published crate (packed by
//! `pack_kit.sh`), else from the repository's `kits/` in a checkout.

use std::path::{Path, PathBuf};

/// What the copy takes along, relative to the kit's crate: the sources, the example, the
/// contract's runners, the manifest and the README.
const PARTS: &[&str] = &[
    "src",
    "examples",
    "tests/fixtures.rs",
    "tests/authorization.rs",
    "Cargo.toml",
    "README.md",
];

fn walk(dir: &Path, base: &Path, out: &mut Vec<(String, PathBuf)>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut entries: Vec<_> = entries.flatten().map(|e| e.path()).collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            walk(&path, base, out);
        } else {
            let relative = path
                .strip_prefix(base)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            out.push((relative, path));
        }
    }
}

fn main() {
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let packed = manifest.join("kit");
    let (kit, fixtures) = if packed.join("src").is_dir() {
        (packed.clone(), packed.join("fixtures"))
    } else {
        (
            manifest.join("../../kits/rust"),
            manifest.join("../../kits/fixtures"),
        )
    };
    let mut files: Vec<(String, PathBuf)> = vec![];
    if kit.join("src").is_dir() {
        // The kit's whole directory, which cargo scans recursively: a file added to or removed
        // from it builds the SDK anew, not only a change to a file already embedded.
        println!("cargo:rerun-if-changed={}", kit.display());
        for part in PARTS {
            // Packed, the kit's manifest is `Cargo.toml.kit`: cargo leaves out any directory
            // with a `Cargo.toml` when it packages the SDK.
            let path = match *part {
                "Cargo.toml" if kit == packed => kit.join("Cargo.toml.kit"),
                _ => kit.join(part),
            };
            if path.is_dir() {
                walk(&path, &kit, &mut files);
            } else if path.exists() {
                files.push(((*part).to_owned(), path));
            }
        }
        println!("cargo:rerun-if-changed={}", fixtures.display());
        let mut found = vec![];
        walk(&fixtures, &fixtures, &mut found);
        files.extend(
            found
                .into_iter()
                .map(|(name, path)| (format!("fixtures/{name}"), path)),
        );
    }
    // The kit carries its own LICENSE, copied next to it.
    let license = if packed.join("LICENSE").exists() {
        packed.join("LICENSE")
    } else {
        manifest.join("../../kits/LICENSE")
    };
    println!("cargo:rerun-if-changed={}", license.display());
    if license.exists() {
        files.push(("LICENSE".into(), license));
    }
    let entries: String = files
        .iter()
        .map(|(name, path)| {
            format!(
                "    ({name:?}, include_bytes!({:?})),\n",
                path.canonicalize().unwrap()
            )
        })
        .collect();
    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("kit_files.rs");
    std::fs::write(out, format!("/// The kit's files: (path in the copy, content).\npub static FILES: &[(&str, &[u8])] = &[\n{entries}];\n")).unwrap();
}
