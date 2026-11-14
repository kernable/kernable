//! `kernable init`: the files kernable Cloud builds and runs an app from. It looks at the
//! directory (a `package.json`, `requirements.txt` or `pyproject.toml`, `Cargo.toml`, `go.mod`,
//! or plain static files), writes a `Containerfile` from the template for what it found, on
//! official images pinned by digest, a `kernable.toml` naming the app's processes, and a
//! `.kernableignore`. Each is the developer's to change; `kernable deploy` builds what they say.
//!
//! `kernable.toml`:
//!
//! ```toml
//! kernel = true                 # the app talks to a kernable kernel (false: it runs without)
//! data = "/data"                # optional: the project's own directory on its volume, kept
//!                               # across restarts and deploys (/data by default without kernel)
//!
//! [process.web]                 # one image, any number of processes, each its command
//! command = ["npm", "start"]
//! port = 3000                   # served over HTTP through the ingress; the process gets PORT
//!
//! [process.worker]
//! command = ["node", "worker.js"]
//!
//! [static]                      # a built frontend inside the image, served by the ingress
//! dir = "/public"
//! spa = true                    # a path without a file gets index.html
//! ```

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

/// What `kernable.toml` says.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct App {
    /// The app talks to a kernable kernel, which runs beside its image.
    #[serde(default = "yes")]
    pub kernel: bool,
    #[serde(default)]
    pub process: BTreeMap<String, Process>,
    #[serde(default, rename = "static", skip_serializing_if = "Option::is_none")]
    pub statics: Option<Static>,
    /// Where the processes find the project's own directory on its volume, read-write, kept
    /// across restarts and deploys: an app without the kernel keeps its state there (`/data`
    /// by default for one).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Process {
    pub command: Vec<String>,
    /// The port it serves HTTP on (it gets it as `PORT`); none: it serves nothing (a worker).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Static {
    /// The directory inside the image.
    pub dir: String,
    #[serde(default)]
    pub spa: bool,
}

fn yes() -> bool {
    true
}

impl App {
    /// Reads and checks `kernable.toml` in `dir`.
    pub fn read(dir: &Path) -> Result<App, String> {
        let path = dir.join("kernable.toml");
        let text = fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
        let app: App = toml::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
        app.check()
            .map_err(|e| format!("{}: {e}", path.display()))?;
        Ok(app)
    }

    /// Process names are short words; every command names something; ports are distinct and
    /// not privileged; something runs or is served.
    pub fn check(&self) -> Result<(), String> {
        if self.process.is_empty() && self.statics.is_none() {
            return Err("no [process.<name>] and no [static]: nothing to run or serve".into());
        }
        let mut ports = vec![];
        for (name, p) in &self.process {
            let ok = !name.is_empty()
                && name.len() <= 32
                && name
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                && name.as_bytes()[0].is_ascii_lowercase();
            if !ok {
                return Err(format!(
                    "process {name:?}: a name of lowercase letters, digits and -, a letter first"
                ));
            }
            if p.command.is_empty() || p.command[0].is_empty() {
                return Err(format!(
                    "process {name}: a command, like [\"npm\", \"start\"]"
                ));
            }
            if let Some(port) = p.port {
                if port < 1024 {
                    return Err(format!("process {name}: port {port}: 1024 or above"));
                }
                if ports.contains(&port) {
                    return Err(format!("process {name}: port {port} is another's"));
                }
                ports.push(port);
            }
        }
        if let Some(s) = &self.statics
            && (!s.dir.starts_with('/') || s.dir.contains(".."))
        {
            return Err(format!(
                "[static] dir {:?}: an absolute path in the image",
                s.dir
            ));
        }
        if let Some(d) = &self.data
            && (!d.starts_with('/')
                || d == "/"
                || d.contains([':', ',', '\n'])
                || d.split('/').any(|p| p == ".."))
        {
            return Err(format!("data {d:?}: an absolute path in the container"));
        }
        Ok(())
    }
}

/// What `init` found in the directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    /// A Node server: its `start` script.
    Node,
    /// A frontend built with `npm run build` into `out` (`dist` by default), no server.
    NodeStatic {
        out: String,
    },
    /// Python: dependencies from `requirements.txt` (or else `pyproject.toml`), run with `main`.
    Python {
        requirements: bool,
        main: String,
    },
    /// A Rust binary named `bin`.
    Rust {
        bin: String,
    },
    Go,
    /// Files served as they are.
    Static,
}

/// What `dir` holds, and whether its app uses the kernable SDK.
pub fn detect(dir: &Path) -> Result<(Kind, bool), String> {
    let read = |name: &str| fs::read_to_string(dir.join(name)).ok();
    if let Some(text) = read("package.json") {
        let pkg: serde_json::Value =
            serde_json::from_str(&text).map_err(|e| format!("package.json: {e}"))?;
        let deps = ["dependencies", "devDependencies"]
            .iter()
            .filter_map(|k| pkg[*k].as_object())
            .flat_map(|m| m.keys().cloned())
            .collect::<Vec<_>>();
        let kernel = deps.iter().any(|d| d == "@kernable/sdk");
        let scripts = &pkg["scripts"];
        if scripts["start"].is_string() {
            return Ok((Kind::Node, kernel));
        }
        if scripts["build"].is_string() {
            let out = if deps.iter().any(|d| d == "react-scripts") {
                "build"
            } else {
                "dist"
            };
            return Ok((Kind::NodeStatic { out: out.into() }, false));
        }
        return Err("package.json has no start script (a server) and no build script (a frontend): add one, then run kernable init again".into());
    }
    let requirements = read("requirements.txt");
    let pyproject = read("pyproject.toml");
    if requirements.is_some() || pyproject.is_some() {
        let all = format!(
            "{}\n{}",
            requirements.clone().unwrap_or_default(),
            pyproject.unwrap_or_default()
        );
        let kernel = all.lines().any(|l| {
            l.trim_start()
                .trim_start_matches('"')
                .starts_with("kernable")
        });
        let main = ["main.py", "app.py", "server.py"]
            .into_iter()
            .find(|m| dir.join(m).exists())
            .unwrap_or("main.py");
        return Ok((
            Kind::Python {
                requirements: requirements.is_some(),
                main: main.into(),
            },
            kernel,
        ));
    }
    if let Some(text) = read("Cargo.toml") {
        let table: toml::Table = text.parse().map_err(|e| format!("Cargo.toml: {e}"))?;
        let bin = table
            .get("package")
            .and_then(|p| p.get("name"))
            .and_then(|n| n.as_str())
            .ok_or("Cargo.toml: no [package] name (a workspace: run kernable init in the binary's crate)")?
            .to_owned();
        let kernel = ["dependencies"].iter().any(|k| {
            table
                .get(*k)
                .and_then(|d| d.as_table())
                .is_some_and(|d| d.keys().any(|n| n == "kernable" || n == "kernable-sdk"))
        });
        return Ok((Kind::Rust { bin }, kernel));
    }
    if let Some(text) = read("go.mod") {
        return Ok((Kind::Go, text.contains("kernable")));
    }
    if dir.join("index.html").exists() {
        return Ok((Kind::Static, false));
    }
    Err("found no package.json, requirements.txt, pyproject.toml, Cargo.toml, go.mod or index.html: write a Containerfile and kernable.toml yourself (kernable init --help shows both)".into())
}

/// The Containerfile and `kernable.toml` for `kind`, on the base images `from` gives (tag, or
/// tag pinned by digest).
pub fn files(kind: &Kind, kernel: bool, from: &dyn Fn(&str) -> String) -> (String, App) {
    let head =
        "# Written by `kernable init`; yours to change. kernable Cloud builds it with Podman.\n";
    let web = |command: &[&str], port: u16| {
        BTreeMap::from([(
            "web".to_owned(),
            Process {
                command: command.iter().map(|s| (*s).to_owned()).collect(),
                port: Some(port),
            },
        )])
    };
    let app = |process, statics| App {
        kernel,
        process,
        statics,
        data: None,
    };
    match kind {
        Kind::Node => (
            format!(
                "{head}FROM {node} AS build\nWORKDIR /app\nCOPY package*.json ./\nRUN npm ci\nCOPY . .\n\
                 RUN npm run build --if-present && npm prune --omit=dev\n\n\
                 FROM {node}\nWORKDIR /app\nENV NODE_ENV=production\nCOPY --from=build /app /app\n\
                 USER node\nCMD [\"npm\", \"start\"]\n",
                node = from("node:22-slim")
            ),
            app(web(&["npm", "start"], 3000), None),
        ),
        Kind::NodeStatic { out } => (
            format!(
                "{head}FROM {node} AS build\nWORKDIR /app\nCOPY package*.json ./\nRUN npm ci\nCOPY . .\n\
                 RUN npm run build\n\n\
                 # Only the built files: the ingress serves them from the image.\n\
                 FROM {busybox}\nCOPY --from=build /app/{out} /public\n",
                node = from("node:22-slim"),
                busybox = from("busybox:stable")
            ),
            app(
                BTreeMap::new(),
                Some(Static {
                    dir: "/public".into(),
                    spa: true,
                }),
            ),
        ),
        Kind::Python { requirements, main } => {
            let install = if *requirements {
                "COPY requirements.txt ./\nRUN pip install --no-cache-dir -r requirements.txt\nCOPY . .\n"
            } else {
                "COPY . .\nRUN pip install --no-cache-dir .\n"
            };
            (
                format!(
                    "{head}FROM {py}\nWORKDIR /app\nENV PYTHONUNBUFFERED=1\n{install}\
                     RUN useradd --create-home app\nUSER app\nCMD [\"python\", \"{main}\"]\n",
                    py = from("python:3.13-slim")
                ),
                app(web(&["python", main], 8000), None),
            )
        }
        Kind::Rust { bin } => (
            format!(
                "{head}FROM {rust} AS build\nWORKDIR /app\nCOPY . .\nRUN cargo build --release --locked\n\n\
                 FROM {debian}\nRUN useradd --create-home app\n\
                 COPY --from=build /app/target/release/{bin} /usr/local/bin/{bin}\n\
                 USER app\nCMD [\"/usr/local/bin/{bin}\"]\n",
                rust = from("rust:1-slim-bookworm"),
                debian = from("debian:bookworm-slim")
            ),
            app(web(&[&format!("/usr/local/bin/{bin}")], 8080), None),
        ),
        Kind::Go => (
            format!(
                "{head}FROM {go} AS build\nWORKDIR /app\nCOPY go.* ./\nRUN go mod download\nCOPY . .\n\
                 RUN CGO_ENABLED=0 go build -o /out/app .\n\n\
                 FROM {debian}\nRUN useradd --create-home app\nCOPY --from=build /out/app /usr/local/bin/app\n\
                 USER app\nCMD [\"/usr/local/bin/app\"]\n",
                go = from("golang:1-bookworm"),
                debian = from("debian:bookworm-slim")
            ),
            app(web(&["/usr/local/bin/app"], 8080), None),
        ),
        Kind::Static => (
            format!(
                "{head}# The files as they are: the ingress serves them from the image.\n\
                 FROM {busybox}\nCOPY . /public\n",
                busybox = from("busybox:stable")
            ),
            app(
                BTreeMap::new(),
                Some(Static {
                    dir: "/public".into(),
                    spa: false,
                }),
            ),
        ),
    }
}

/// What is never sent to be built.
pub const IGNORE: &str = "# Not sent to kernable Cloud's build (as .gitignore reads it).\n\
.git\nnode_modules\ntarget\n__pycache__\n.venv\n.env\n.env.*\n*.key\n*.pem\n";

/// `image:tag` pinned by its digest on Docker Hub (`image:tag@sha256:…`), or `None` when the
/// registry cannot be asked.
pub fn pin(image: &str) -> Option<String> {
    let (repo, tag) = image.split_once(':')?;
    let repo = if repo.contains('/') {
        repo.to_owned()
    } else {
        format!("library/{repo}")
    };
    let agent = ureq::Agent::new_with_config(
        ureq::Agent::config_builder()
            .timeout_global(Some(std::time::Duration::from_secs(10)))
            .build(),
    );
    let text = agent
        .get(format!(
            "https://auth.docker.io/token?service=registry.docker.io&scope=repository:{repo}:pull"
        ))
        .call()
        .ok()?
        .body_mut()
        .read_to_string()
        .ok()?;
    let token: serde_json::Value = serde_json::from_str(&text).ok()?;
    let token = token["token"].as_str()?;
    let response = agent
        .head(format!(
            "https://registry-1.docker.io/v2/{repo}/manifests/{tag}"
        ))
        .header("authorization", &format!("Bearer {token}"))
        .header(
            "accept",
            "application/vnd.oci.image.index.v1+json, \
             application/vnd.docker.distribution.manifest.list.v2+json, \
             application/vnd.oci.image.manifest.v1+json, \
             application/vnd.docker.distribution.manifest.v2+json",
        )
        .call()
        .ok()?;
    let digest = response
        .headers()
        .get("docker-content-digest")?
        .to_str()
        .ok()?;
    digest
        .starts_with("sha256:")
        .then(|| format!("{image}@{digest}"))
}

/// `kernable init [<dir>] [--force] [--offline]`: writes the files; what it did, line by line.
pub fn run(dir: &Path, force: bool, offline: bool) -> Result<Vec<String>, String> {
    let (kind, kernel) = detect(dir)?;
    let unpinned = std::cell::RefCell::new(Vec::<String>::new());
    let from = |image: &str| -> String {
        if offline {
            return image.to_owned();
        }
        pin(image).unwrap_or_else(|| image.to_owned())
    };
    let (containerfile, app) = files(&kind, kernel, &|image| {
        let pinned = from(image);
        if !pinned.contains('@') {
            unpinned.borrow_mut().push(image.to_owned());
        }
        pinned
    });
    let mut unpinned = unpinned.into_inner();
    app.check()?;
    let mut containerfile = containerfile;
    if !unpinned.is_empty() {
        unpinned.dedup();
        containerfile.push_str(&format!(
            "\n# Not pinned by digest yet ({}): pin each FROM as image:tag@sha256:… \
             (kernable init --force pins them when the registry answers).\n",
            unpinned.join(", ")
        ));
    }
    let toml_text = format!(
        "# How kernable Cloud runs this app: one image, its processes. See `kernable init --help`.\n{}",
        toml::to_string(&app).map_err(|e| e.to_string())?
    );
    let write = |name: &str, text: &str| -> Result<Option<String>, String> {
        let path: PathBuf = dir.join(name);
        if path.exists() && !force {
            return Ok(Some(format!("kept {name} (--force writes it anew)")));
        }
        fs::write(&path, text).map_err(|e| format!("{}: {e}", path.display()))?;
        Ok(None)
    };
    let mut done = vec![format!(
        "found {}{}",
        match &kind {
            Kind::Node => "a Node server".to_owned(),
            Kind::NodeStatic { out } => format!("a frontend built into {out}/"),
            Kind::Python { main, .. } => format!("a Python app ({main})"),
            Kind::Rust { bin } => format!("a Rust binary ({bin})"),
            Kind::Go => "a Go program".to_owned(),
            Kind::Static => "static files".to_owned(),
        },
        if kernel { ", using the kernel" } else { "" }
    )];
    for (name, text) in [
        ("Containerfile", containerfile.as_str()),
        ("kernable.toml", toml_text.as_str()),
        (".kernableignore", IGNORE),
    ] {
        match write(name, text)? {
            Some(kept) => done.push(kept),
            None => done.push(format!("wrote {name}")),
        }
    }
    if !unpinned.is_empty() {
        done.push(format!("not pinned by digest: {}", unpinned.join(", ")));
    }
    Ok(done)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(name: &str, files: &[(&str, &str)]) -> PathBuf {
        let d = std::env::temp_dir().join(format!("kn-init-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        for (f, text) in files {
            fs::write(d.join(f), text).unwrap();
        }
        d
    }

    #[test]
    fn each_kind_of_app_is_found_with_whether_it_uses_the_kernel() {
        let node = dir(
            "node",
            &[(
                "package.json",
                r#"{"scripts":{"start":"node server.js"},"dependencies":{"@kernable/sdk":"1"}}"#,
            )],
        );
        assert_eq!(detect(&node).unwrap(), (Kind::Node, true));
        let vite = dir(
            "vite",
            &[(
                "package.json",
                r#"{"scripts":{"build":"vite build"},"devDependencies":{"vite":"5"}}"#,
            )],
        );
        assert_eq!(
            detect(&vite).unwrap(),
            (Kind::NodeStatic { out: "dist".into() }, false)
        );
        let py = dir(
            "py",
            &[
                ("requirements.txt", "kernable>=0.1\nfastapi\n"),
                ("app.py", ""),
            ],
        );
        assert_eq!(
            detect(&py).unwrap(),
            (
                Kind::Python {
                    requirements: true,
                    main: "app.py".into()
                },
                true
            )
        );
        let rust = dir(
            "rust",
            &[(
                "Cargo.toml",
                "[package]\nname = \"shop\"\n[dependencies]\nkernable-sdk = \"0.1\"\n",
            )],
        );
        assert_eq!(
            detect(&rust).unwrap(),
            (Kind::Rust { bin: "shop".into() }, true)
        );
        let site = dir("site", &[("index.html", "<h1>hi</h1>")]);
        assert_eq!(detect(&site).unwrap(), (Kind::Static, false));
        assert!(detect(&dir("none", &[])).is_err());
        for d in [node, vite, py, rust, site] {
            let _ = fs::remove_dir_all(d);
        }
    }

    #[test]
    fn the_files_name_processes_ports_and_static_files() {
        let tag = |i: &str| i.to_owned();
        let (c, app) = files(&Kind::Node, true, &tag);
        assert!(c.contains("FROM node:22-slim AS build") && c.contains("USER node"));
        assert_eq!(app.process["web"].port, Some(3000));
        app.check().unwrap();
        let (c, app) = files(&Kind::NodeStatic { out: "dist".into() }, false, &tag);
        assert!(c.contains("COPY --from=build /app/dist /public"));
        assert!(app.process.is_empty() && app.statics.as_ref().unwrap().spa);
        app.check().unwrap();
        let (c, app) = files(&Kind::Rust { bin: "shop".into() }, true, &tag);
        assert!(c.contains("/app/target/release/shop") && c.contains("USER app"));
        assert_eq!(app.process["web"].command, ["/usr/local/bin/shop"]);
        // The kernable.toml a template writes reads back as it was.
        let text = toml::to_string(&app).unwrap();
        let back: App = toml::from_str(&text).unwrap();
        assert_eq!(back, app);
    }

    #[test]
    fn a_kernable_toml_names_something_to_run_with_distinct_ports() {
        let read = |text: &str| -> Result<App, String> {
            let app: App = toml::from_str(text).map_err(|e| e.to_string())?;
            app.check().map(|()| app)
        };
        let web = "[process.web]\ncommand = [\"x\"]\n";
        assert!(read(&format!("kernel = false\ndata = \"/var/lib/app\"\n{web}")).is_ok());
        assert!(read(&format!("data = \"data\"\n{web}")).is_err());
        assert!(read(&format!("data = \"/a/../b\"\n{web}")).is_err());
        assert!(
            read("kernel = true\n")
                .unwrap_err()
                .contains("nothing to run")
        );
        assert!(
            read("[process.web]\ncommand = [\"x\"]\nport = 80\n")
                .unwrap_err()
                .contains("1024")
        );
        assert!(
            read("[process.a]\ncommand = [\"x\"]\nport = 3000\n[process.b]\ncommand = [\"y\"]\nport = 3000\n")
                .unwrap_err()
                .contains("another's")
        );
        assert!(read("[process.Web]\ncommand = [\"x\"]\n").is_err());
        assert!(read("[static]\ndir = \"public\"\n").is_err());
        let app = read("[process.worker]\ncommand = [\"node\", \"w.js\"]\n").unwrap();
        assert!(app.kernel, "kernel by default");
    }
}
