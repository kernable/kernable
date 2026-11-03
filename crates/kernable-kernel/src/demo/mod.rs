//! `kernable demo`: kernable shown scene by scene, for presentations only.
//!
//! The demo runs a real kernel, `kernable start --dev` as a child process, on a store in a new
//! temporary directory, and the bank of [`bank`] in this process with the Rust SDK: a worker, a
//! read model and a client. Its UI is the kernel's own, with the Stage (`/demo`) on top; every
//! other path goes to the kernel's UI. Reset stops both and starts them again on a new store;
//! nothing of the demo outlives the process, and it never touches an existing store.
//!
//! The Stage asks with `POST /api/demo/next` and `POST /api/demo/reset`, each with an
//! `x-kernable-demo` header (a page elsewhere cannot send one without asking first), and reads
//! `GET /api/demo` and `GET /api/demo/code`.

mod bank;

use std::{
    collections::HashMap,
    net::TcpListener as StdListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};

use kernable::{App, AppHandle, Kernable, Options, Outcome};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{BoxError, http::reply};

const STAGE: &str = include_str!("stage.html");
const UI: &str = include_str!("../ui.html");
const BANK: &str = include_str!("bank.rs");

/// How long a new kernel and the bank may take to come up.
const START_TIMEOUT: Duration = Duration::from_secs(30);
/// How long the kernel's UI may take to answer a request passed on to it.
const PROXY_TIMEOUT: Duration = Duration::from_secs(10);

/// A scene of the Stage: steps on one aggregate type, shown with one read model.
struct Scene {
    title: &'static str,
    /// What the Stage says before the first step.
    intro: &'static str,
    aggregate: &'static str,
    read_model: &'static str,
    /// The read model's query for one aggregate, `{ "id": … }`.
    query: &'static str,
    steps: &'static [Step],
}

/// One step of a scene: what the client sends, and what the Stage says about it. Several
/// commands in one step are sent at the same moment.
struct Step {
    title: &'static str,
    says: &'static str,
    sends: &'static [Send],
    /// The regions of `bank.rs` the code panel shows, the handler first.
    code: &'static [&'static str],
}

/// One command of a step.
struct Send {
    id: &'static str,
    command: &'static str,
    data: &'static str,
}

const fn send(id: &'static str, command: &'static str, data: &'static str) -> Send {
    Send { id, command, data }
}

const SCENES: &[Scene] = &[
    // A command becomes an event, the event a row in a read model, and the client reads its own
    // write.
    Scene {
        title: "Command → event → read model",
        intro: "A client sends a command, a worker decides, the kernel stores the event, a read model shows it.",
        aggregate: "account",
        read_model: "accounts",
        query: "GetAccount",
        steps: &[
            Step {
                title: "Open an account",
                says: "A client opens an account for Ada. The worker decides, the kernel stores the event, the read model shows it.",
                sends: &[send("a1", "OpenAccount", r#"{"owner":"Ada"}"#)],
                code: &["open", "project-opened", "client"],
            },
            Step {
                title: "Deposit 100",
                says: "The submit returns only once the read model has the event, so the query right after it already sees the new balance.",
                sends: &[send("a1", "Deposit", r#"{"amount":100}"#)],
                code: &["deposit", "project-deposited", "client"],
            },
            Step {
                title: "Withdraw 500",
                says: "The worker rejects the command. Nothing is stored: no event, no change in the read model.",
                sends: &[send("a1", "Withdraw", r#"{"amount":500}"#)],
                code: &["withdraw", "client"],
            },
            Step {
                title: "Withdraw 30",
                says: "The balance comes from the events alone: 100 deposited, 30 withdrawn. Every event is signed and chained to the one before.",
                sends: &[send("a1", "Withdraw", r#"{"amount":30}"#)],
                code: &["withdraw", "project-withdrawn", "events", "query"],
            },
        ],
    },
    // A unique key: never two holders, also when two commands race for it.
    Scene {
        title: "Two customers, one username",
        intro: "A username belongs to one customer only. The worker never checks it: the kernel does, when it commits, also when two commands race for it.",
        aggregate: "customer",
        read_model: "customers",
        query: "GetCustomer",
        steps: &[
            Step {
                title: "Ada registers as ada",
                says: "The customer aggregate declares its username unique. With the event, the kernel stores that c1 now holds the key.",
                sends: &[send("c1", "Register", r#"{"name":"Ada","username":"ada"}"#)],
                code: &["register", "customer-key", "project-registered"],
            },
            Step {
                title: "Bob wants ada too",
                says: "The worker accepts: it never looks the username up. The kernel refuses the commit, because c1 holds the key.",
                sends: &[send("c2", "Register", r#"{"name":"Bob","username":"ADA"}"#)],
                code: &["register", "customer-key", "client"],
            },
            Step {
                title: "The race: Cleo and Dan, one username",
                says: "Two customers claim cleo at the same moment. Both workers accept; the kernel commits one at a time, so exactly one wins.",
                sends: &[
                    send("c3", "Register", r#"{"name":"Cleo","username":"cleo"}"#),
                    send("c4", "Register", r#"{"name":"Dan","username":"cleo"}"#),
                ],
                code: &["register", "customer-key", "project-registered"],
            },
            Step {
                title: "Ada changes her username",
                says: "The key follows the state: once c1 is ada.lovelace, it no longer holds ada, in the same commit as the event.",
                sends: &[send(
                    "c1",
                    "ChangeUsername",
                    r#"{"username":"ada.lovelace"}"#,
                )],
                code: &["rename", "customer-key", "project-renamed"],
            },
            Step {
                title: "Bob tries again",
                says: "The same command as before now commits: ada is free. No cleanup job, no claims table, no compensation.",
                sends: &[send("c2", "Register", r#"{"name":"Bob","username":"ada"}"#)],
                code: &["register", "customer-key", "project-registered"],
            },
        ],
    },
];

/// What the bank's handlers saw of each command, by command ID: when it reached the worker
/// (and the state the handler got) and when the read model, the first time only, as a command
/// may run again.
#[derive(Clone, Default)]
pub(crate) struct Recorder(Arc<Mutex<HashMap<String, Seen>>>);

#[derive(Default)]
struct Seen {
    worker: Option<Instant>,
    state: Option<Value>,
    read_model: Option<Instant>,
}

impl Recorder {
    pub(crate) fn worker(&self, command_id: &str, state: &impl serde::Serialize) {
        let mut seen = lock(&self.0);
        let seen = seen.entry(command_id.to_owned()).or_default();
        if seen.worker.is_none() {
            seen.worker = Some(Instant::now());
            seen.state = serde_json::to_value(state).ok();
        }
    }

    pub(crate) fn read_model(&self, command_id: &str) {
        let mut seen = lock(&self.0);
        let at = &mut seen.entry(command_id.to_owned()).or_default().read_model;
        at.get_or_insert_with(Instant::now);
    }

    fn take(&self, command_id: &str) -> Seen {
        lock(&self.0).remove(command_id).unwrap_or_default()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One kernel with the bank on it: replaced whole on reset.
struct Run {
    app: AppHandle,
    client: Kernable,
    stage: Recorder,
    rows: bank::Rows,
    /// Where the kernel serves its own UI.
    ui: String,
    // Dropped in this order: the kernel stops before its directory goes.
    _kernel: Process,
    _dir: Dir,
}

impl Drop for Run {
    fn drop(&mut self) {
        self.app.stop();
    }
}

/// The kernel process, killed when dropped, and on Unix its watchdog: a shell reading a pipe
/// from this process that kills the kernel once the pipe closes, so the kernel ends with the
/// demo even if the demo is killed.
struct Process(Child, Option<Child>);

impl Process {
    fn spawn(command: &mut Command) -> std::io::Result<Process> {
        let kernel = command.spawn()?;
        let watchdog = if cfg!(unix) {
            Command::new("sh")
                .args([
                    "-c",
                    &format!("read _; kill -9 {} 2>/dev/null", kernel.id()),
                ])
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .ok()
        } else {
            None
        };
        Ok(Process(kernel, watchdog))
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
        if let Some(watchdog) = &mut self.1 {
            let _ = watchdog.kill();
            let _ = watchdog.wait();
        }
    }
}

/// A run's directory, removed when dropped.
struct Dir(PathBuf);

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Default)]
struct State {
    /// Counts the runs; the Stage starts over when it changes.
    generation: u64,
    run: Option<Arc<Run>>,
    /// What runs now (a step or a reset), if anything.
    busy: Option<&'static str>,
    /// The scene shown, an index into [`SCENES`].
    scene: usize,
    /// What each step did, in order: the next step is `traces.len()`.
    traces: Vec<Value>,
    error: Option<String>,
}

struct Demo {
    base: PathBuf,
    state: Mutex<State>,
}

/// Runs the demo until Ctrl-C, its UI on `ui` (a loopback address).
pub async fn run(ui: &str) -> Result<(), BoxError> {
    if !crate::loopback(ui) {
        return Err(
            format!("--ui-listen {ui}: the demo listens on a loopback address only").into(),
        );
    }
    let base = std::env::temp_dir().join(format!("kernable-demo-{}", std::process::id()));
    std::fs::create_dir_all(&base)?;
    let demo = Arc::new(Demo {
        base,
        state: Mutex::default(),
    });
    let first = start(&demo, 1).await;
    demo.started(1, first);
    if let Some(e) = &lock(&demo.state).error {
        return Err(e.clone().into());
    }
    eprintln!(
        "kernable demo: open http://{ui}/demo (the kernel's UI at http://{ui}/). For presentations \
         only: every reset starts on a new, empty store. Ctrl-C ends it."
    );
    let server = {
        let demo = Arc::clone(&demo);
        crate::http::serve_with(
            ui,
            |_| {},
            move |request| {
                let demo = Arc::clone(&demo);
                async move { demo.respond(&request).await }
            },
        )
    };
    let outcome = tokio::select! {
        result = server => result,
        _ = tokio::signal::ctrl_c() => Ok(()),
    };
    lock(&demo.state).run = None;
    let _ = std::fs::remove_dir_all(&demo.base);
    outcome
}

impl Demo {
    async fn respond(self: &Arc<Self>, request: &[u8]) -> Vec<u8> {
        let text = String::from_utf8_lossy(request);
        let mut words = text.split_whitespace();
        let (method, target) = (words.next().unwrap_or(""), words.next().unwrap_or(""));
        let path = target.split('?').next().unwrap_or("");
        let asked = text
            .lines()
            .take_while(|line| !line.is_empty())
            .any(|line| line.to_ascii_lowercase().starts_with("x-kernable-demo:"));
        match (method, path) {
            ("GET", "/") => reply("200 OK", "text/html; charset=utf-8", UI.as_bytes()),
            ("GET", "/demo") => reply("200 OK", "text/html; charset=utf-8", STAGE.as_bytes()),
            ("GET", "/api/demo") => json_reply(&self.view()),
            ("GET", "/api/demo/code") => json_reply(&code()),
            ("POST", _) if !asked => reply(
                "403 Forbidden",
                "text/plain",
                b"the demo takes requests from its Stage only\n",
            ),
            ("POST", "/api/demo/next") => self.next(),
            ("POST", "/api/demo/reset") => self.reset(None),
            ("POST", "/api/demo/scene") => {
                let n = target
                    .split_once("?n=")
                    .and_then(|(_, n)| n.parse::<usize>().ok());
                match n {
                    Some(n) if n >= 1 => self.reset(Some(n - 1)),
                    _ => conflict("name a scene: /api/demo/scene?n=1"),
                }
            }
            ("GET", _) => {
                let ui = lock(&self.state).run.as_ref().map(|r| r.ui.clone());
                match ui {
                    Some(ui) => proxy(&ui, request).await,
                    None => reply("503 Service Unavailable", "text/plain", b"resetting\n"),
                }
            }
            _ => reply("404 Not Found", "text/plain", b"not found\n"),
        }
    }

    fn view(&self) -> Value {
        let state = lock(&self.state);
        let scene = &SCENES[state.scene];
        let rows = match &state.run {
            Some(r) if scene.read_model == "customers" => json!(*lock(&r.rows.customers)),
            Some(r) => json!(*lock(&r.rows.accounts)),
            None => json!({}),
        };
        let steps: Vec<Value> = scene
            .steps
            .iter()
            .map(|s| {
                let sends: Vec<Value> = s
                    .sends
                    .iter()
                    .map(|c| json!({ "id": c.id, "command": c.command, "data": data(c) }))
                    .collect();
                json!({ "title": s.title, "says": s.says, "sends": sends, "code": s.code })
            })
            .collect();
        json!({
            "generation": state.generation,
            "ready": state.run.is_some(),
            "busy": state.busy,
            "error": state.error,
            "scenes": SCENES.iter().map(|s| s.title).collect::<Vec<_>>(),
            "scene": {
                "index": state.scene,
                "title": scene.title,
                "intro": scene.intro,
                "aggregate": scene.aggregate,
                "query": scene.query,
                "readModel": scene.read_model,
                "steps": steps,
            },
            "traces": state.traces,
            "rows": rows,
        })
    }

    /// Starts the next step, unless one is running or the scene is over.
    fn next(self: &Arc<Self>) -> Vec<u8> {
        let (run, scene, index, generation) = {
            let mut state = lock(&self.state);
            let index = state.traces.len();
            let Some(run) = state.run.clone() else {
                return conflict("the demo is starting");
            };
            if state.busy.is_some() {
                return conflict("a step is running");
            }
            if index >= SCENES[state.scene].steps.len() {
                return conflict("the scene is over: reset to start again");
            }
            state.busy = Some("step");
            (run, &SCENES[state.scene], index, state.generation)
        };
        let demo = Arc::clone(self);
        tokio::spawn(async move {
            let trace = step(&run, scene, generation, index).await;
            let mut state = lock(&demo.state);
            if state.generation == generation {
                state.traces.push(trace);
                state.busy = None;
            }
        });
        json_reply(&json!({ "step": index }))
    }

    /// Replaces the kernel and the bank with new ones on a new store, for `scene` if given.
    fn reset(self: &Arc<Self>, scene: Option<usize>) -> Vec<u8> {
        let generation = {
            let mut state = lock(&self.state);
            if state.busy == Some("reset") {
                return conflict("already resetting");
            }
            if let Some(scene) = scene {
                if scene >= SCENES.len() {
                    return conflict("no such scene");
                }
                state.scene = scene;
            }
            state.generation += 1;
            state.busy = Some("reset");
            state.traces.clear();
            state.error = None;
            // The old run stops here, or once a step still using it ends.
            state.run = None;
            state.generation
        };
        let demo = Arc::clone(self);
        tokio::spawn(async move {
            let run = start(&demo, generation).await;
            demo.started(generation, run);
        });
        json_reply(&json!({ "generation": generation }))
    }

    fn started(&self, generation: u64, run: Result<Run, BoxError>) {
        let mut state = lock(&self.state);
        state.generation = generation;
        state.busy = None;
        match run {
            Ok(run) => state.run = Some(Arc::new(run)),
            Err(e) => state.error = Some(format!("the demo kernel did not start: {e}")),
        }
    }
}

/// Starts a kernel on a new store in its own directory, and the bank on it.
async fn start(demo: &Demo, generation: u64) -> Result<Run, BoxError> {
    let dir = Dir(demo.base.join(format!("run-{generation}")));
    std::fs::create_dir_all(&dir.0)?;
    let (grpc, ui) = (free_address()?, free_address()?);
    let log = dir.0.join("kernel.log");
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args(["start", "--dev", "--listen", &grpc, "--ui-listen", &ui])
        .current_dir(&dir.0)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(&log)?);
    // Only the flags above: nothing from a kernel configured in this shell.
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("KERNABLE_") {
            command.env_remove(name);
        }
    }
    command.env("KERNABLE_LOG", "warn");
    let mut kernel = Process::spawn(&mut command)?;
    let deadline = Instant::now() + START_TIMEOUT;
    while TcpStream::connect(&grpc).await.is_err() {
        if let Some(status) = kernel.0.try_wait()? {
            let log = std::fs::read_to_string(&log).unwrap_or_default();
            return Err(format!("the kernel exited ({status}): {}", log.trim()).into());
        }
        if Instant::now() > deadline {
            return Err("the kernel did not listen within 30 s".into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let (stage, rows) = (Recorder::default(), bank::Rows::default());
    let client = Kernable::new(
        Options::new(dir.0.join("app.key"))
            .url(format!("http://{grpc}"))
            .service("demo"),
    )?;
    let app = client.app(
        App::new()
            .service("demo")
            .description("the bank of kernable demo")
            .domain(&bank::bank(&stage, &rows)),
    )?;
    let running = {
        let app = app.clone();
        tokio::spawn(async move { app.run().await })
    };
    if tokio::time::timeout(START_TIMEOUT, app.ready())
        .await
        .is_err()
    {
        running.abort();
        return Err("the bank did not connect within 30 s".into());
    }
    Ok(Run {
        app,
        client,
        stage,
        rows,
        ui,
        _kernel: kernel,
        _dir: dir,
    })
}

/// Runs step `index` of `scene` and says what happened to each of its commands, sent at the
/// same moment.
async fn step(run: &Arc<Run>, scene: &'static Scene, generation: u64, index: usize) -> Value {
    let s = &scene.steps[index];
    let mut tasks = Vec::new();
    for (n, c) in s.sends.iter().enumerate() {
        let run = Arc::clone(run);
        let command_id = format!("demo-{generation}-{}-{}", index + 1, n + 1);
        tasks.push(tokio::spawn(async move {
            sent(&run, scene, c, command_id).await
        }));
    }
    let mut sends = Vec::new();
    for task in tasks {
        sends.push(
            task.await
                .unwrap_or_else(|e| json!({ "outcome": "failed", "error": e.to_string() })),
        );
    }
    json!({ "step": index, "sends": sends })
}

/// Sends one command and says what happened, with the time each point took from the submit,
/// in microseconds.
async fn sent(run: &Run, scene: &Scene, c: &Send, command_id: String) -> Value {
    let data = data(c);
    let sent_at = Instant::now();
    let sent = bank::send(
        &run.client,
        &command_id,
        (scene.aggregate, c.id, c.command),
        data.clone(),
        scene.query,
    )
    .await;
    let seen = run.stage.take(&command_id);
    let micros = |at: Option<Instant>| at.map(|at| at.duration_since(sent_at).as_micros() as u64);
    let (outcome, events, rejection, error) = match &sent.outcome {
        Ok(Outcome::Committed(events)) => {
            let events: Vec<Value> = events
                .iter()
                .map(|e| {
                    json!({
                        "position": e.position,
                        "type": e.event_type,
                        "version": e.version,
                        "data": e.data,
                    })
                })
                .collect();
            ("committed", events, Value::Null, Value::Null)
        }
        Ok(Outcome::Rejected(r)) => (
            "rejected",
            Vec::new(),
            json!({ "code": r.code, "message": r.message, "key": r.key }),
            Value::Null,
        ),
        Err(e) => ("failed", Vec::new(), Value::Null, json!(e.to_string())),
    };
    let answer = match &sent.answer {
        Ok(value) => json!({ "row": value }),
        Err(e) => json!({ "error": e.to_string() }),
    };
    json!({
        "commandId": command_id,
        "command": c.command,
        "id": c.id,
        "data": data,
        "outcome": outcome,
        "events": events,
        "rejection": rejection,
        "error": error,
        "answer": answer,
        // The aggregate as the worker rebuilt it from its events, before this command.
        "state": seen.state,
        "micros": {
            "worker": micros(seen.worker),
            "readModel": micros(seen.read_model),
            "returned": micros(Some(sent.returned)),
            "answered": micros(Some(sent.answered)),
        },
    })
}

fn data(c: &Send) -> Value {
    serde_json::from_str(c.data).unwrap_or(Value::Null)
}

/// The regions of `bank.rs` by name, each a list of lines; lines that only record for the Stage
/// are marked, so it can dim them. Regions of the same name follow each other.
fn code() -> Value {
    let mut regions: Vec<(String, Vec<Value>)> = Vec::new();
    let mut open: Option<String> = None;
    for line in BANK.lines() {
        let trimmed = line.trim();
        if let Some(name) = trimmed.strip_prefix("// #region ") {
            open = Some(name.to_owned());
            match regions.iter_mut().find(|(n, _)| n == name) {
                Some((_, lines)) => lines.push(json!({ "text": "", "stage": false })),
                None => regions.push((name.to_owned(), Vec::new())),
            }
        } else if trimmed.starts_with("// #endregion") {
            open = None;
        } else if let Some(name) = &open
            && let Some((_, lines)) = regions.iter_mut().find(|(n, _)| n == name)
        {
            let stage = trimmed.ends_with("// stage");
            let text = line.trim_end_matches("// stage").trim_end();
            lines.push(json!({ "text": text, "stage": stage }));
        }
    }
    let regions: serde_json::Map<String, Value> = regions
        .into_iter()
        .map(|(name, lines)| (name, Value::Array(lines)))
        .collect();
    json!({ "file": "crates/kernable-kernel/src/demo/bank.rs", "regions": regions })
}

/// Passes a request on to the kernel's UI and returns its answer.
async fn proxy(addr: &str, request: &[u8]) -> Vec<u8> {
    let forward = async {
        let mut conn = TcpStream::connect(addr).await?;
        conn.write_all(request).await?;
        let mut out = Vec::new();
        conn.read_to_end(&mut out).await?;
        Ok::<_, std::io::Error>(out)
    };
    match tokio::time::timeout(PROXY_TIMEOUT, forward).await {
        Ok(Ok(out)) => out,
        _ => reply(
            "502 Bad Gateway",
            "text/plain",
            b"the kernel's UI did not answer\n",
        ),
    }
}

/// A loopback address no one listens on now.
fn free_address() -> std::io::Result<String> {
    let listener = StdListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.to_string())
}

fn json_reply(value: &Value) -> Vec<u8> {
    reply("200 OK", "application/json", value.to_string().as_bytes())
}

fn conflict(why: &str) -> Vec<u8> {
    reply(
        "409 Conflict",
        "application/json",
        json!({ "error": why }).to_string().as_bytes(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_code_panel_gets_every_region_a_step_names() {
        let code = code();
        let regions = code["regions"].as_object().unwrap();
        for step in SCENES.iter().flat_map(|s| s.steps) {
            for name in step.code {
                assert!(regions.contains_key(*name), "{} shows {name}", step.title);
            }
        }
        let open: Vec<&Value> = regions["open"].as_array().unwrap().iter().collect();
        assert!(open.iter().any(|l| l["stage"] == true));
        assert!(
            open.iter()
                .all(|l| !l["text"].as_str().unwrap().contains("// stage")),
            "the marker itself is not shown"
        );
    }

    #[test]
    fn every_step_sends_json() {
        for c in SCENES.iter().flat_map(|s| s.steps).flat_map(|s| s.sends) {
            serde_json::from_str::<Value>(c.data).unwrap();
        }
    }
}
