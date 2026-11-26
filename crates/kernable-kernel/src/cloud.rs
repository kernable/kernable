//! `kernable cloud …`: the client of a kernable Cloud installation's account API. It only
//! speaks HTTP to the API it is given (`--api`, `KERNABLE_CLOUD_API`, or the one it signed in
//! to), so the same CLI serves every installation.
//!
//! Sign-in is by device code (RFC 8628): the CLI shows a code and a page, the person signs in
//! there in any browser and approves, the CLI gets a session of its own. Its tokens are kept in
//! the OS keychain where there is one (macOS `security`, Linux `secret-tool`), else in
//! `~/.config/kernable/cloud.json`, readable by its owner only. CI sets
//! `KERNABLE_CLOUD_TOKEN` to a team's token instead.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const USAGE: &str = "\
kernable Cloud, through its account API (--api <url>, KERNABLE_CLOUD_API, or the one signed in to):
  kernable cloud login [--api <url>] [--no-browser]
                                          Signs in by device code: open the page shown, enter the
                                          code, sign in and approve. Keeps the session in the OS
                                          keychain, else in ~/.config/kernable/cloud.json (0600)
  kernable cloud logout                   Ends the session and forgets it
  kernable cloud whoami                   The account, its teams and roles; a CI token's team
  kernable cloud teams [create <slug> [<name>] | accept-terms <team> | verify-payment <team>
                       | billing <team> | delete <team> | restore <team> | two-factor <team> on|off]
                                          Lists your teams, or opens one; accepts the terms and
                                          the Preview Access notice, sets up the team's billing
                                          (a payment page to open, where the installation bills),
                                          opens its billing portal (invoices, payment method,
                                          cancelling), deletes a team (kept 30 days, restorable),
                                          requires a second factor of every member (owners')
  kernable cloud members [--team <team>] [invite <email> --role <role> | role <identity> <role>
                       | remove <identity>]
                                          Roles: owner, admin, developer, viewer
  kernable cloud tokens [--team <team>] [create <label> --role <role> [--project <project>]
                       [--expires <days>] | revoke <id>]
                                          Tokens for CI (KERNABLE_CLOUD_TOKEN), never an owner's;
                                          shown once
  kernable cloud audit [--team <team>] [--before <position>] [--limit <n>]
                                          The team's audit log, newest first: who changed what
                                          (admins and owners)
  kernable cloud usage [--team <team>] [--month <yyyy-mm>]
                                          The team's copies this month: GB-months stored, GB sent
                                          to readers, against what the monthly fee includes
  kernable cloud projects [--team <team>] [create <name> | show <name> | usage <name> [--month <yyyy-mm>]]
                                          A project is your team's record its kernels link to
                                          (kernable Cloud runs no code of yours); past the checks
                                          before a team's first project
  kernable cloud projects [--team <team>] links <name>
                                          Its partitions linked to your kernels, each key the relay
                                          takes with its role and state, and the relay they push to
  kernable cloud projects [--team <team>] root <name> [set <root.pub|ID> [--now] | finish]
                                          The project's root key: grants signed by it count for its
                                          kernels and copies. set replaces it and keeps the old one
                                          valid until finish (7 days at most); --now ends the old
                                          one at once, for a key that leaked (admins)
  kernable cloud projects [--team <team>] link <name> <partition> --pusher <pusher.pub|ID>
                                          Lets the kernel with that pusher key push the partition's
                                          copy to the relay (developers and up); prints the relay.
                                          A linked partition takes another pusher only once its
                                          current one is stopped or revoked
  kernable cloud projects [--team <team>] key <name> <partition> <key.pub|ID> (read | push | stop | revoke | activate)
                                          read, push: adds the key in that role (a reader, a
                                          writer's pusher), active; stop: refused until activated
                                          again; revoke: refused for good. The relay applies it
                                          within seconds (developers and up). Copy keys never
                                          pass through kernable Cloud: give each instance its
                                          partition's copy key yourself
  kernable cloud projects [--team <team>] unlink <name> <partition>
                                          Nothing pushes to or reads the copy from then on; it is
                                          kept 30 days, linking again brings it back (admins)
  kernable cloud projects [--team <team>] node-token <name> <instance>
                                          A one-time join token (24 h) for a managed node (admins)
  kernable cloud projects [--team <team>] alerts <name> [set <email>...]
                                          Where the project's alerts go: always the team's owners
                                          and admins, and these addresses (at most 10; set with
                                          none leaves only the owners and admins; admins)
  --team defaults to your personal team; --json prints the API's answer as it is.";

const DEFAULT_API: &str = "https://api.kernable.cloud";
const KEYCHAIN_SERVICE: &str = "kernable-cloud";

/// What `cloud.json` holds: the API and, without a keychain, the session's tokens.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Stored {
    api: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tokens: Option<Tokens>,
    /// The tokens are in the OS keychain under the API's address.
    #[serde(default)]
    keychain: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Tokens {
    access_token: String,
    refresh_token: String,
}

fn config_dir() -> Result<PathBuf, String> {
    if let Some(x) = std::env::var_os("XDG_CONFIG_HOME").filter(|x| !x.is_empty()) {
        return Ok(PathBuf::from(x).join("kernable"));
    }
    let home = std::env::var_os("HOME").ok_or("HOME is not set")?;
    Ok(PathBuf::from(home).join(".config").join("kernable"))
}

fn stored_path() -> Result<PathBuf, String> {
    Ok(config_dir()?.join("cloud.json"))
}

fn load() -> Result<Option<Stored>, String> {
    let path = stored_path()?;
    match std::fs::read_to_string(&path) {
        Ok(text) => serde_json::from_str(&text)
            .map(Some)
            .map_err(|e| format!("{}: {e}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("{}: {e}", path.display())),
    }
}

fn save(stored: &Stored) -> Result<(), String> {
    use std::os::unix::fs::OpenOptionsExt;
    let dir = config_dir()?;
    std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let path = stored_path()?;
    let tmp = path.with_extension("json.tmp");
    let text = serde_json::to_string_pretty(stored).map_err(|e| e.to_string())?;
    let _ = std::fs::remove_file(&tmp);
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)
        .map_err(|e| format!("{}: {e}", tmp.display()))?;
    f.write_all(text.as_bytes())
        .and_then(|_| f.sync_all())
        .map_err(|e| format!("{}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, &path).map_err(|e| format!("{}: {e}", path.display()))
}

// --- the OS keychain -----------------------------------------------------------------------------

fn keychain_wanted() -> bool {
    std::env::var("KERNABLE_CLOUD_KEYCHAIN").as_deref() != Ok("0")
}

/// Runs `program` with `args`, `input` on its stdin; its stdout if it succeeded.
fn tool(program: &str, args: &[&str], input: Option<&str>) -> Option<String> {
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    if let Some(stdin) = child.stdin.as_mut() {
        let _ = stdin.write_all(input.unwrap_or_default().as_bytes());
    }
    drop(child.stdin.take());
    let out = child.wait_with_output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim_end().to_owned())
}

/// Keeps `secret` for `api` in the keychain; whether it could.
fn keychain_set(api: &str, secret: &str) -> bool {
    if !keychain_wanted() {
        return false;
    }
    if cfg!(target_os = "macos") {
        // `security` takes the password as an argument only (or on a terminal): visible in
        // the process list for the moment it runs, to this user's processes.
        tool(
            "security",
            &[
                "add-generic-password",
                "-U",
                "-a",
                api,
                "-s",
                KEYCHAIN_SERVICE,
                "-w",
                secret,
            ],
            None,
        )
        .is_some()
    } else {
        tool(
            "secret-tool",
            &[
                "store",
                "--label=kernable cloud",
                "service",
                KEYCHAIN_SERVICE,
                "account",
                api,
            ],
            Some(secret),
        )
        .is_some()
    }
}

fn keychain_get(api: &str) -> Option<String> {
    if cfg!(target_os = "macos") {
        tool(
            "security",
            &[
                "find-generic-password",
                "-a",
                api,
                "-s",
                KEYCHAIN_SERVICE,
                "-w",
            ],
            None,
        )
    } else {
        tool(
            "secret-tool",
            &["lookup", "service", KEYCHAIN_SERVICE, "account", api],
            None,
        )
    }
    .filter(|s| !s.is_empty())
}

fn keychain_clear(api: &str) {
    if cfg!(target_os = "macos") {
        tool(
            "security",
            &["delete-generic-password", "-a", api, "-s", KEYCHAIN_SERVICE],
            None,
        );
    } else {
        tool(
            "secret-tool",
            &["clear", "service", KEYCHAIN_SERVICE, "account", api],
            None,
        );
    }
}

// --- the session ---------------------------------------------------------------------------------

struct Session {
    api: String,
    /// `Bearer …` or `ApiKey …`.
    auth: Option<String>,
    tokens: Option<Tokens>,
    keychain: bool,
    json: bool,
}

/// Whether `api` may be spoken to: https, or http to this machine only (tokens travel).
fn check_api(api: &str) -> Result<String, String> {
    let api = api.trim_end_matches('/').to_owned();
    if api.starts_with("https://") {
        return Ok(api);
    }
    let rest = api
        .strip_prefix("http://")
        .ok_or_else(|| format!("--api {api}: an https:// address"))?;
    let host = rest.split('/').next().unwrap_or_default();
    let host = match host.rsplit_once(':') {
        Some((h, _)) if !h.ends_with(']') || h.starts_with('[') => h,
        _ => host,
    };
    if ["localhost", "127.0.0.1", "[::1]"].contains(&host) {
        Ok(api)
    } else {
        Err(format!(
            "--api {api}: http only to this machine; any other address over https"
        ))
    }
}

impl Session {
    fn open(api_flag: Option<&str>, json: bool) -> Result<Session, String> {
        let stored = load()?;
        let api = api_flag
            .map(str::to_owned)
            .or_else(|| std::env::var("KERNABLE_CLOUD_API").ok())
            .or_else(|| stored.as_ref().map(|s| s.api.clone()))
            .unwrap_or_else(|| DEFAULT_API.to_owned());
        let api = check_api(&api)?;
        if let Ok(token) = std::env::var("KERNABLE_CLOUD_TOKEN")
            && !token.is_empty()
        {
            return Ok(Session {
                api,
                auth: Some(format!("ApiKey {token}")),
                tokens: None,
                keychain: false,
                json,
            });
        }
        let stored = stored.filter(|s| s.api == api);
        let (tokens, keychain) = match &stored {
            Some(s) if s.keychain => (
                keychain_get(&api).and_then(|t| serde_json::from_str(&t).ok()),
                true,
            ),
            Some(s) => (s.tokens.clone(), false),
            None => (None, false),
        };
        Ok(Session {
            auth: tokens
                .as_ref()
                .map(|t| format!("Bearer {}", t.access_token)),
            api,
            tokens,
            keychain,
            json,
        })
    }

    fn keep(&mut self, tokens: Tokens) -> Result<(), String> {
        let text = serde_json::to_string(&tokens).map_err(|e| e.to_string())?;
        let keychain = keychain_set(&self.api, &text);
        save(&Stored {
            api: self.api.clone(),
            tokens: (!keychain).then(|| tokens.clone()),
            keychain,
        })?;
        self.auth = Some(format!("Bearer {}", tokens.access_token));
        self.tokens = Some(tokens);
        self.keychain = keychain;
        Ok(())
    }

    fn forget(&mut self) -> Result<(), String> {
        if self.keychain {
            keychain_clear(&self.api);
        }
        save(&Stored {
            api: self.api.clone(),
            tokens: None,
            keychain: false,
        })?;
        self.tokens = None;
        self.auth = None;
        Ok(())
    }

    fn raw(
        &self,
        method: &str,
        path: &str,
        auth: Option<&str>,
        body: Option<&Value>,
    ) -> Result<(u16, Value), String> {
        let agent = ureq::Agent::new_with_config(
            ureq::Agent::config_builder()
                .http_status_as_error(false)
                .timeout_global(Some(Duration::from_secs(60)))
                .build(),
        );
        let url = format!("{}{path}", self.api);
        let mut request = ureq::http::Request::builder()
            .method(method)
            .uri(&url)
            .header(
                "user-agent",
                format!("kernable-cli/{}", env!("CARGO_PKG_VERSION")),
            )
            .header("content-type", "application/json");
        if let Some(a) = auth {
            request = request.header("authorization", a);
        }
        let body = body.map(Value::to_string).unwrap_or_default();
        let request = request.body(body).map_err(|e| format!("{url}: {e}"))?;
        let mut response = agent.run(request).map_err(|e| format!("{url}: {e}"))?;
        let status = response.status().as_u16();
        let text = response
            .body_mut()
            .read_to_string()
            .map_err(|e| format!("{url}: {e}"))?;
        let value = serde_json::from_str(&text).unwrap_or(Value::String(text));
        Ok((status, value))
    }

    /// Calls the API as the signed-in person or the CI token; an expired access token is
    /// refreshed once (the refresh token rotates).
    fn call(&mut self, method: &str, path: &str, body: Option<Value>) -> Result<Value, String> {
        if self.auth.is_none() {
            return Err(format!(
                "not signed in to {}: kernable cloud login (or KERNABLE_CLOUD_TOKEN in CI)",
                self.api
            ));
        }
        let (mut status, mut value) =
            self.raw(method, path, self.auth.as_deref(), body.as_ref())?;
        if status == 401
            && let Some(t) = self.tokens.clone()
        {
            let (s, fresh) = self.raw(
                "POST",
                "/v1/sessions/refresh",
                None,
                Some(&json!({ "refresh_token": t.refresh_token })),
            )?;
            if s != 200 {
                self.forget()?;
                return Err("the session ended: kernable cloud login".into());
            }
            let tokens = Tokens {
                access_token: text(&fresh, "access_token"),
                refresh_token: text(&fresh, "refresh_token"),
            };
            self.keep(tokens)?;
            (status, value) = self.raw(method, path, self.auth.as_deref(), body.as_ref())?;
        }
        if (200..300).contains(&status) {
            return Ok(value);
        }
        let message = value["message"]
            .as_str()
            .or_else(|| value["error"].as_str())
            .map(str::to_owned)
            .unwrap_or_else(|| value.to_string());
        Err(format!("{method} {path}: {status}: {message}"))
    }

    fn show(&self, value: &Value, human: impl FnOnce(&Value)) {
        if self.json {
            println!(
                "{}",
                serde_json::to_string_pretty(value).unwrap_or_default()
            );
        } else {
            human(value)
        }
    }
}

fn text(v: &Value, field: &str) -> String {
    v[field].as_str().unwrap_or_default().to_owned()
}

fn hostname() -> String {
    let mut buf = [0u8; 256];
    // SAFETY: the buffer is valid for its length; gethostname writes at most that.
    let ok = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) } == 0;
    let end = buf.iter().position(|b| *b == 0).unwrap_or(buf.len());
    if ok {
        String::from_utf8_lossy(&buf[..end]).into_owned()
    } else {
        "this machine".into()
    }
}

fn open_browser(url: &str) {
    let program = if cfg!(target_os = "macos") {
        "open"
    } else {
        "xdg-open"
    };
    if cfg!(target_os = "linux")
        && std::env::var_os("DISPLAY").is_none()
        && std::env::var_os("WAYLAND_DISPLAY").is_none()
    {
        return;
    }
    let _ = Command::new(program)
        .arg(url)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
}

// --- the commands --------------------------------------------------------------------------------

/// Takes `--flag <value>` out of `args`.
fn flag(args: &mut Vec<String>, name: &str) -> Result<Option<String>, String> {
    let Some(i) = args.iter().position(|a| a == name) else {
        return Ok(None);
    };
    if i + 1 >= args.len() {
        return Err(format!("{name}: a value"));
    }
    let value = args.remove(i + 1);
    args.remove(i);
    Ok(Some(value))
}

fn switch(args: &mut Vec<String>, name: &str) -> bool {
    match args.iter().position(|a| a == name) {
        Some(i) => {
            args.remove(i);
            true
        }
        None => false,
    }
}

/// The team: `--team`, else the personal team of the person signed in.
fn team(s: &mut Session, given: Option<String>) -> Result<String, String> {
    if let Some(t) = given {
        return Ok(t);
    }
    let me = s.call("GET", "/v1/me", None)?;
    if let Some(t) = me["team"].as_str() {
        return Ok(t.to_owned());
    }
    me["teams"]
        .as_array()
        .and_then(|teams| teams.iter().find(|t| t["personal"] == true))
        .and_then(|t| t["slug"].as_str())
        .map(str::to_owned)
        .ok_or_else(|| "no personal team: give --team <team>".into())
}

/// Runs `kernable cloud <args>`; false: wrong arguments (exit 2).
pub fn run(args: &[String]) -> Result<bool, String> {
    let mut args = args.to_vec();
    let api = flag(&mut args, "--api")?;
    let json = switch(&mut args, "--json");
    let team_flag = flag(&mut args, "--team")?;
    let words: Vec<&str> = args.iter().map(String::as_str).collect();
    match words.as_slice() {
        ["login", rest @ ..] => {
            let no_browser = rest.contains(&"--no-browser");
            if rest.iter().any(|a| *a != "--no-browser") {
                return Ok(false);
            }
            login(Session::open(api.as_deref(), json)?, no_browser)
        }
        ["logout"] => {
            let mut s = Session::open(api.as_deref(), json)?;
            if s.tokens.is_some() {
                let _ = s.call("DELETE", "/v1/sessions", None);
            }
            s.forget()?;
            println!("signed out of {}", s.api);
            Ok(true)
        }
        ["whoami"] => {
            let mut s = Session::open(api.as_deref(), json)?;
            let me = s.call("GET", "/v1/me", None)?;
            let api = s.api.clone();
            s.show(&me, |me| {
                if let Some(team) = me["team"].as_str() {
                    println!("a CI token of team {team} at {api}: {}", me["token"]);
                    return;
                }
                println!(
                    "account {} at {api}{}{}",
                    text(me, "account"),
                    if me["verified"] == true {
                        ""
                    } else {
                        " (email not verified)"
                    },
                    if me["second_factor"] == true {
                        ", second factor proved"
                    } else {
                        ""
                    }
                );
                if me["preview_access"] == true {
                    println!(
                        "Preview Access: no SLA, no on-call; support in business hours, best effort"
                    );
                }
                for t in me["teams"].as_array().into_iter().flatten() {
                    print_team(t);
                }
            });
            Ok(true)
        }
        ["teams"] => {
            let mut s = Session::open(api.as_deref(), json)?;
            let teams = s.call("GET", "/v1/teams", None)?;
            s.show(&teams, |t| {
                t.as_array().into_iter().flatten().for_each(print_team)
            });
            Ok(true)
        }
        ["teams", "create", slug, name @ ..] if name.len() <= 1 => {
            let mut s = Session::open(api.as_deref(), json)?;
            let body = json!({ "slug": slug, "name": name.first().copied().unwrap_or(slug) });
            let t = s.call("POST", "/v1/teams", Some(body))?;
            s.show(&t, print_team);
            Ok(true)
        }
        [
            "teams",
            verb @ ("accept-terms" | "verify-payment" | "delete" | "restore"),
            team,
        ] => {
            let mut s = Session::open(api.as_deref(), json)?;
            let (method, path) = match *verb {
                "accept-terms" => ("POST", format!("/v1/teams/{team}/agreements")),
                "verify-payment" => ("POST", format!("/v1/teams/{team}/payment-method")),
                "delete" => ("DELETE", format!("/v1/teams/{team}")),
                _ => ("POST", format!("/v1/teams/{team}/restore")),
            };
            let v = s.call(method, &path, None)?;
            s.show(&v, |v| match *verb {
                "accept-terms" => println!("{team}: the terms and the Preview Access notice are accepted"),
                "verify-payment" => match v["url"].as_str() {
                    Some(url) => {
                        eprintln!("{team}: set up billing on this page; `kernable cloud whoami` shows when it is done");
                        println!("{url}");
                    }
                    None => println!("{team}: a payment method is marked verified"),
                },
                "delete" => println!(
                    "{team}: suspended, its projects stopped; deleted after the notice (restore until then: kernable cloud teams restore {team}){}",
                    v["deleted_at"].as_i64().map(|_| "").unwrap_or_default()
                ),
                _ => println!("{team}: restored, its projects running again"),
            });
            Ok(true)
        }
        ["teams", "billing", team] => {
            let mut s = Session::open(api.as_deref(), json)?;
            let v = s.call("POST", &format!("/v1/teams/{team}/billing/portal"), None)?;
            s.show(&v, |v| {
                eprintln!("{team}: invoices, the payment method and cancelling, on this page:");
                println!("{}", text(v, "url"));
            });
            Ok(true)
        }
        ["teams", "two-factor", team, on @ ("on" | "off")] => {
            let mut s = Session::open(api.as_deref(), json)?;
            let body = json!({ "required": *on == "on" });
            let v = s.call("PUT", &format!("/v1/teams/{team}/two-factor"), Some(body))?;
            s.show(&v, |_| match *on {
                "on" => println!("{team}: every member acts only with a second factor"),
                _ => println!("{team}: a second factor for owners only"),
            });
            Ok(true)
        }
        ["members", rest @ ..] => members(Session::open(api.as_deref(), json)?, team_flag, rest),
        ["tokens", rest @ ..] => tokens(Session::open(api.as_deref(), json)?, team_flag, rest),
        ["projects", rest @ ..] => projects(Session::open(api.as_deref(), json)?, team_flag, rest),
        ["audit", rest @ ..] => audit(Session::open(api.as_deref(), json)?, team_flag, rest),
        ["usage", rest @ ..] => usage(Session::open(api.as_deref(), json)?, team_flag, rest),
        ["help" | "--help"] | [] => {
            println!("{USAGE}");
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn print_team(t: &Value) {
    let missing: Vec<&str> = t["missing"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    println!(
        "{}\t{}\t{}{}{}",
        text(t, "slug"),
        text(t, "role"),
        text(t, "name"),
        if t["personal"] == true {
            " (personal)"
        } else {
            ""
        },
        if missing.is_empty() {
            String::new()
        } else {
            format!("\tbefore a first project: {}", missing.join(", "))
        }
    );
}

fn login(mut s: Session, no_browser: bool) -> Result<bool, String> {
    let (status, start) = s.raw(
        "POST",
        "/v1/cli/device",
        None,
        Some(&json!({ "client": hostname() })),
    )?;
    if status != 200 {
        return Err(format!("{}: {status}: {start}", s.api));
    }
    let code = text(&start, "user_code");
    let page = text(&start, "verification_uri");
    let complete = text(&start, "verification_uri_complete");
    let device = text(&start, "device_code");
    let mut interval = start["interval"].as_u64().unwrap_or(5).max(1);
    let expires = start["expires_in"].as_u64().unwrap_or(600);
    if start["preview_access"] == true {
        eprintln!(
            "kernable Cloud, Preview Access: no SLA, no on-call; support in business hours, best effort."
        );
    }
    eprintln!("Open {page} and enter the code {code}");
    if !no_browser {
        open_browser(&complete);
    }
    let deadline = Instant::now() + Duration::from_secs(expires);
    loop {
        std::thread::sleep(Duration::from_secs(interval));
        if Instant::now() > deadline {
            return Err("the code expired: kernable cloud login again".into());
        }
        let (status, answer) = s.raw(
            "POST",
            "/v1/cli/token",
            None,
            Some(&json!({ "device_code": device })),
        )?;
        if status == 200 {
            s.keep(Tokens {
                access_token: text(&answer, "access_token"),
                refresh_token: text(&answer, "refresh_token"),
            })?;
            break;
        }
        match answer["error"].as_str() {
            Some("authorization_pending") => {}
            Some("slow_down") => interval += 5,
            Some("access_denied") => return Err("denied on the page".into()),
            Some("expired_token") => {
                return Err("the code expired: kernable cloud login again".into());
            }
            _ => return Err(format!("{}: {status}: {answer}", s.api)),
        }
    }
    let me = s.call("GET", "/v1/me", None)?;
    eprintln!(
        "signed in to {} as account {} (session kept in {})",
        s.api,
        text(&me, "account"),
        if s.keychain {
            "the keychain"
        } else {
            "~/.config/kernable/cloud.json"
        }
    );
    Ok(true)
}

fn members(mut s: Session, team_flag: Option<String>, rest: &[&str]) -> Result<bool, String> {
    let mut rest: Vec<String> = rest.iter().map(|w| (*w).to_owned()).collect();
    let role = flag(&mut rest, "--role")?;
    let t = team(&mut s, team_flag)?;
    let words: Vec<&str> = rest.iter().map(String::as_str).collect();
    match (words.as_slice(), role) {
        ([], None) => {
            let list = s.call("GET", &format!("/v1/teams/{t}/members"), None)?;
            s.show(&list, |l| {
                for m in l.as_array().into_iter().flatten() {
                    println!(
                        "{}\t{}\t{}",
                        text(m, "identity"),
                        m["role"].as_str().unwrap_or("-"),
                        m["email"].as_str().or(m["account"].as_str()).unwrap_or("-")
                    );
                }
            });
        }
        (["invite", email], Some(role)) => {
            let v = s.call(
                "POST",
                &format!("/v1/teams/{t}/invitations"),
                Some(json!({ "email": email, "role": role })),
            )?;
            s.show(&v, |_| {
                println!("{email} is invited to {t} as {role}: the invitation is mailed")
            });
        }
        (["role", identity, role], None) => {
            let v = s.call(
                "PUT",
                &format!("/v1/teams/{t}/members/{identity}"),
                Some(json!({ "role": role })),
            )?;
            s.show(&v, |_| println!("{identity} is a {role} of {t}"));
        }
        (["remove", identity], None) => {
            let v = s.call("DELETE", &format!("/v1/teams/{t}/members/{identity}"), None)?;
            s.show(&v, |_| println!("{identity} holds no role in {t} any more"));
        }
        _ => return Ok(false),
    }
    Ok(true)
}

/// The team's audit log: one line per entry, newest first; how to page further on stderr.
/// `kernable cloud usage`: the team's copies this month (or `--month`), against what the
/// monthly fee includes.
fn usage(mut s: Session, team_flag: Option<String>, rest: &[&str]) -> Result<bool, String> {
    let mut rest: Vec<String> = rest.iter().map(|w| (*w).to_owned()).collect();
    let month = flag(&mut rest, "--month")?;
    if !rest.is_empty() {
        return Ok(false);
    }
    let t = team(&mut s, team_flag)?;
    let path = match &month {
        Some(m) => format!("/v1/teams/{t}/usage?month={m}"),
        None => format!("/v1/teams/{t}/usage"),
    };
    let v = s.call("GET", &path, None)?;
    s.show(&v, print_usage);
    Ok(true)
}

/// A month's usage as the customer service answers it: a line per copy, then the total and
/// what the fee includes.
fn print_usage(v: &Value) {
    let n = |v: &Value| v.as_f64().unwrap_or_default();
    let copies = v["copies"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    for c in copies {
        println!(
            "{}	holds {:.3} GB	stored {:.3} GB-months	sent {:.3} GB",
            text(c, "copy"),
            n(&c["stored_gb"]),
            n(&c["stored_gb_months"]),
            n(&c["sent_gb"]),
        );
    }
    if copies.is_empty() {
        println!("nothing counted in {}", text(v, "month"));
    }
    println!(
        "{} total	stored {:.3} GB-months (included {})	sent {:.3} GB (included {})",
        text(v, "month"),
        n(&v["total"]["stored_gb_months"]),
        n(&v["included"]["stored_gb_months"]),
        n(&v["total"]["sent_gb"]),
        n(&v["included"]["sent_gb"]),
    );
}

fn audit(mut s: Session, team_flag: Option<String>, rest: &[&str]) -> Result<bool, String> {
    let mut rest: Vec<String> = rest.iter().map(|w| (*w).to_owned()).collect();
    let before = flag(&mut rest, "--before")?;
    let limit = flag(&mut rest, "--limit")?.unwrap_or_else(|| "50".into());
    if !rest.is_empty() {
        return Ok(false);
    }
    for (name, value) in [
        ("--before", before.as_deref()),
        ("--limit", Some(limit.as_str())),
    ] {
        if value.is_some_and(|v| v.parse::<u64>().is_err()) {
            return Err(format!("{name}: a number"));
        }
    }
    let t = team(&mut s, team_flag)?;
    let mut path = format!("/v1/teams/{t}/audit?limit={limit}");
    if let Some(b) = &before {
        path.push_str(&format!("&before={b}"));
    }
    let list = s.call("GET", &path, None)?;
    s.show(&list, |l| {
        let entries = l.as_array().map(Vec::as_slice).unwrap_or_default();
        for e in entries {
            let named = |name: &str, raw: &str| {
                e[name].as_str().map_or_else(|| text(e, raw), str::to_owned)
            };
            println!(
                "{}\t{}\t{}\t{}\t{}",
                crate::config::utc(e["at"].as_i64().unwrap_or_default()),
                named("actor_name", "actor"),
                text(e, "action"),
                named("subject_name", "subject"),
                text(e, "detail"),
            );
        }
        if let Some(last) = entries
            .last()
            .filter(|_| entries.len().to_string() == limit)
        {
            eprintln!(
                "older: kernable cloud audit --team {t} --before {}",
                last["position"]
            );
        }
    });
    Ok(true)
}

fn tokens(mut s: Session, team_flag: Option<String>, rest: &[&str]) -> Result<bool, String> {
    let mut rest: Vec<String> = rest.iter().map(|w| (*w).to_owned()).collect();
    let role = flag(&mut rest, "--role")?;
    let project = flag(&mut rest, "--project")?;
    let expires = flag(&mut rest, "--expires")?
        .map(|d| {
            d.trim_end_matches('d')
                .parse::<i64>()
                .map_err(|_| format!("--expires {d}: days"))
        })
        .transpose()?;
    let t = team(&mut s, team_flag)?;
    let words: Vec<&str> = rest.iter().map(String::as_str).collect();
    match (words.as_slice(), role) {
        ([], None) => {
            let list = s.call("GET", &format!("/v1/teams/{t}/tokens"), None)?;
            s.show(&list, |l| {
                for k in l.as_array().into_iter().flatten() {
                    println!(
                        "{}\t{}\t{}\t{}{}",
                        text(k, "identity"),
                        text(k, "label"),
                        text(k, "role"),
                        k["project"].as_str().unwrap_or("the whole team"),
                        if k["revoked"] == true {
                            "\trevoked"
                        } else {
                            ""
                        }
                    );
                }
            });
        }
        (["create", label], Some(role)) => {
            let v = s.call(
                "POST",
                &format!("/v1/teams/{t}/tokens"),
                Some(json!({ "label": label, "role": role, "project": project, "expires_days": expires })),
            )?;
            s.show(&v, |v| {
                eprintln!("token {} ({role}): shown once, keep it in your CI's secrets as KERNABLE_CLOUD_TOKEN", text(v, "id"));
                println!("{}", text(v, "token"));
            });
        }
        (["revoke", id], None) => {
            let v = s.call("DELETE", &format!("/v1/teams/{t}/tokens/{id}"), None)?;
            s.show(&v, |_| println!("token {id} is revoked"));
        }
        _ => return Ok(false),
    }
    Ok(true)
}

/// Where a project's alerts go, besides the team's owners and admins.
fn print_alert_to(name: &str, to: &serde_json::Value) {
    match to.as_array().filter(|a| !a.is_empty()) {
        None => println!("{name}: alerts go to the team's owners and admins"),
        Some(to) => println!(
            "{name}: alerts go to the team's owners and admins, and to {}",
            to.iter()
                .filter_map(|a| a.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn projects(mut s: Session, team_flag: Option<String>, rest: &[&str]) -> Result<bool, String> {
    let mut rest: Vec<String> = rest.iter().map(|w| (*w).to_owned()).collect();
    let month = flag(&mut rest, "--month")?;
    let pusher = flag(&mut rest, "--pusher")?;
    let now = switch(&mut rest, "--now");
    let t = team(&mut s, team_flag)?;
    let words: Vec<&str> = rest.iter().map(String::as_str).collect();
    match words.as_slice() {
        [] => {
            let list = s.call("GET", &format!("/v1/teams/{t}/projects"), None)?;
            s.show(&list, |l| {
                for p in l.as_array().into_iter().flatten() {
                    println!(
                        "{}\t{}\t{}{}",
                        text(p, "name"),
                        text(p, "plan"),
                        text(p, "status"),
                        p["reason"]
                            .as_str()
                            .map(|r| format!("\t{r}"))
                            .unwrap_or_default()
                    );
                }
            });
        }
        ["create", name] => {
            let v = s.call(
                "POST",
                &format!("/v1/teams/{t}/projects"),
                Some(json!({ "name": name })),
            )?;
            s.show(&v, |v| println!("{name}: {}", text(v, "status")));
        }
        ["show", name] => {
            let v = s.call("GET", &format!("/v1/teams/{t}/projects/{name}"), None)?;
            s.show(&v, |v| {
                let p = &v["project"];
                println!("{name}\t{}", text(p, "status"));
            });
        }
        ["usage", name] => {
            let path = match &month {
                Some(m) => format!("/v1/teams/{t}/projects/{name}/usage?month={m}"),
                None => format!("/v1/teams/{t}/projects/{name}/usage"),
            };
            let v = s.call("GET", &path, None)?;
            s.show(&v, print_usage);
        }
        ["links", name] => {
            let v = s.call("GET", &format!("/v1/teams/{t}/projects/{name}/links"), None)?;
            s.show(&v, |v| {
                println!("relay: {}", text(v, "relay"));
                for l in v["links"].as_array().into_iter().flatten() {
                    let state = match l["removed_at"].as_i64() {
                        Some(_) => "unlinked (kept 30 days)",
                        None => "linked",
                    };
                    println!("{}\t{state}", text(l, "copy"));
                    for k in l["keys"].as_array().into_iter().flatten() {
                        let role = match k["role"].as_str() {
                            Some("push") => "pusher",
                            _ => "reader",
                        };
                        println!("  {}\t{role}\t{}", text(k, "key"), text(k, "state"));
                    }
                }
            });
        }
        ["link", name, partition] => {
            let pusher = pusher.as_deref().ok_or("--pusher <pusher.pub|ID>")?;
            let body = json!({ "pusher": key_id_of(pusher)? });
            let path = format!("/v1/teams/{t}/projects/{name}/links/{partition}");
            let v = s.call("PUT", &path, Some(body))?;
            s.show(&v, |v| {
                println!(
                    "{}: linked; push it with: kernable push … --to {} --copy {}",
                    text(&v["link"], "copy"),
                    text(v, "relay"),
                    text(&v["link"], "copy")
                )
            });
        }
        ["key", name, partition, key, what] => {
            let body = match *what {
                "read" | "push" => json!({ "role": what, "state": "active" }),
                "stop" => json!({ "state": "stopped" }),
                "revoke" => json!({ "state": "revoked" }),
                "activate" => json!({ "state": "active" }),
                other => return Err(format!("{other}: read, push, stop, revoke or activate")),
            };
            let key = key_id_of(key)?;
            let path = format!("/v1/teams/{t}/projects/{name}/links/{partition}/keys/{key}");
            let v = s.call("PUT", &path, Some(body))?;
            s.show(&v, |v| {
                let k = v["link"]["keys"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .find(|k| k["key"] == key.as_str());
                if let Some(k) = k {
                    println!(
                        "{partition}: {key} {} {}",
                        text(k, "role"),
                        text(k, "state")
                    );
                }
            });
        }
        ["root", name] => {
            let v = s.call("GET", &format!("/v1/teams/{t}/projects/{name}"), None)?;
            s.show(&v, |v| print_root(name, &v["project"]));
        }
        ["root", name, "set", key] => {
            let path = format!("/v1/teams/{t}/projects/{name}/root");
            let v = s.call(
                "PUT",
                &path,
                Some(json!({ "key": key_id_of(key)?, "now": now })),
            )?;
            s.show(&v, |v| print_root(name, &v["project"]));
        }
        ["root", name, "finish"] => {
            let path = format!("/v1/teams/{t}/projects/{name}/root/finish");
            let v = s.call("POST", &path, None)?;
            s.show(&v, |v| print_root(name, &v["project"]));
        }
        ["alerts", name] => {
            let v = s.call("GET", &format!("/v1/teams/{t}/projects/{name}"), None)?;
            s.show(&v, |v| print_alert_to(name, &v["project"]["alert_to"]));
        }
        ["alerts", name, "set", to @ ..] => {
            let path = format!("/v1/teams/{t}/projects/{name}/alerts");
            let v = s.call("PUT", &path, Some(json!({ "to": to })))?;
            s.show(&v, |v| print_alert_to(name, &v["alert_to"]));
        }
        ["unlink", name, partition] => {
            let path = format!("/v1/teams/{t}/projects/{name}/links/{partition}");
            let v = s.call("DELETE", &path, None)?;
            s.show(&v, |v| {
                println!(
                    "{}: unlinked; kept 30 days, link it again to bring it back",
                    text(v, "copy")
                )
            });
        }
        ["node-token", name, instance] => {
            let path = format!("/v1/teams/{t}/projects/{name}/nodes/tokens");
            let v = s.call("POST", &path, Some(json!({ "instance": instance })))?;
            s.show(&v, |v| {
                println!(
                    "once, within 24 hours, on {instance}'s machine:\n  kernable node join https://control.kernable.cloud {}",
                    text(v, "token")
                )
            });
        }
        _ => return Ok(false),
    }
    Ok(true)
}

fn print_root(name: &str, p: &Value) {
    match p["root"]["key_id"].as_str() {
        Some(id) => println!("{name}: root {id}"),
        None => println!(
            "{name}: no root key yet; set it with: kernable cloud projects root {name} set root.pub"
        ),
    }
    if let Some(old) = p["previous_root"]["key_id"].as_str() {
        let until = utc(p["previous_root"]["until"].as_i64().unwrap_or_default());
        println!(
            "  the old root {old} stays valid until {until}, or until: kernable cloud projects root {name} finish"
        );
    }
}

/// Milliseconds since the epoch as `YYYY-MM-DD HH:MM UTC`.
fn utc(ms: i64) -> String {
    let (days, rem) = (
        ms.div_euclid(86_400_000),
        ms.rem_euclid(86_400_000) / 60_000,
    );
    // Days to a civil date (H. Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02} {:02}:{:02} UTC", rem / 60, rem % 60)
}

/// A key ID given as itself (64 hex digits) or as a `.pub` file holding one. Never another file:
/// a `.key` holds 64 hex digits too, and it is a secret.
fn key_id_of(given: &str) -> Result<String, String> {
    let hex = |s: &str| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit());
    if hex(given) {
        return Ok(given.to_ascii_lowercase());
    }
    if !given.ends_with(".pub") {
        return Err(format!(
            "{given}: a key ID (64 hex digits) or a .pub file; never a .key, it is secret"
        ));
    }
    let text = std::fs::read_to_string(given).map_err(|e| format!("{given}: {e}"))?;
    let id = text.trim();
    if hex(id) {
        Ok(id.to_ascii_lowercase())
    } else {
        Err(format!("{given}: no key ID in it"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times_are_printed_in_utc() {
        assert_eq!(utc(0), "1970-01-01 00:00 UTC");
        assert_eq!(utc(1_796_124_480_000), "2026-12-01 11:28 UTC");
        assert_eq!(utc(951_782_400_000), "2000-02-29 00:00 UTC");
    }

    #[test]
    fn the_api_is_https_or_this_machine() {
        assert_eq!(
            check_api("https://api.example.com/").unwrap(),
            "https://api.example.com"
        );
        assert!(check_api("http://127.0.0.1:8480").is_ok());
        assert!(check_api("http://localhost").is_ok());
        assert!(check_api("http://api.example.com").is_err());
        assert!(check_api("http://localhost.example.com:80").is_err());
        assert!(check_api("ftp://x").is_err());
    }

    #[test]
    fn a_key_id_comes_from_itself_or_a_pub_file_never_a_key() {
        let dir = std::env::temp_dir().join(format!("kn-cli-ids-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let id = "AB".repeat(32);
        assert_eq!(key_id_of(&id).unwrap(), "ab".repeat(32));
        let public = dir.join("pusher.pub");
        let secret = dir.join("pusher.key");
        std::fs::write(&public, format!("{}\n", "cd".repeat(32))).unwrap();
        std::fs::write(&secret, "ef".repeat(32)).unwrap();
        assert_eq!(
            key_id_of(public.to_str().unwrap()).unwrap(),
            "cd".repeat(32)
        );
        let err = key_id_of(secret.to_str().unwrap()).unwrap_err();
        assert!(err.contains("never a .key"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn flags_come_out_of_the_words() {
        let mut args: Vec<String> = ["tokens", "create", "ci", "--role", "developer"]
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        assert_eq!(
            flag(&mut args, "--role").unwrap().as_deref(),
            Some("developer")
        );
        assert_eq!(args, ["tokens", "create", "ci"]);
        assert!(flag(&mut vec!["--role".to_owned()], "--role").is_err());
        assert!(!run(&["nonsense".to_owned()]).unwrap());
    }

    #[test]
    fn the_session_file_is_its_owners_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("kn-cloud-cli-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        // SAFETY: this test alone reads XDG_CONFIG_HOME in this process.
        unsafe { std::env::set_var("XDG_CONFIG_HOME", &dir) };
        save(&Stored {
            api: "http://127.0.0.1:1".into(),
            tokens: Some(Tokens {
                access_token: "a".into(),
                refresh_token: "r".into(),
            }),
            keychain: false,
        })
        .unwrap();
        let path = stored_path().unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let back = load().unwrap().unwrap();
        assert_eq!(back.tokens.unwrap().refresh_token, "r");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
