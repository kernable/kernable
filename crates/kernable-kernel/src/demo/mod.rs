//! `kernable demo`: kernable shown scene by scene, for presentations only.
//!
//! The demo runs a real kernel, `kernable start --dev` as a child process, on a store in a new
//! temporary directory, and the parts of the bank ([`bank`]) the scene needs in this process
//! with the Rust SDK: workers, read models, reactors, a middleware and a client. Its UI is the
//! kernel's own, with the Stage (`/demo`) on top; every other path goes to the kernel's UI.
//! Reset stops both and starts them again on a new store; nothing of the demo outlives the
//! process, and it never touches an existing store.
//!
//! The Stage asks with `POST /api/demo/next` and `POST /api/demo/reset`, each with an
//! `x-kernable-demo` header (a page elsewhere cannot
//! send one without asking first), and reads `GET /api/demo`.

mod act;
mod bank;
mod scenes;

use std::{
    collections::HashMap,
    net::TcpListener as StdListener,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};

use kernable::{App, AppHandle, Kernable, Options, Outcome};
use scenes::{Does, SCENES, Scene, Send};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{
    BoxError,
    http::{Answer, reply},
};

const STAGE: &str = include_str!("stage.html");
const UI: &str = include_str!("../ui.html");

/// How long a new kernel and the bank may take to come up.
const START_TIMEOUT: Duration = Duration::from_secs(30);
/// How long the kernel's UI may take to answer a request (to accept one passed on to it).
const PROXY_TIMEOUT: Duration = Duration::from_secs(10);

/// What the bank's handlers saw of each command, by command ID: when it first reached the
/// worker (and the state the handler got), how often it ran, and when it first reached the read
/// model.
#[derive(Clone, Default)]
pub(crate) struct Recorder(Arc<Mutex<HashMap<String, Seen>>>);

#[derive(Default)]
struct Seen {
    worker: Option<Instant>,
    state: Option<Value>,
    runs: u32,
    read_model: Option<Instant>,
}

impl Recorder {
    pub(crate) fn worker(&self, command_id: &str, state: &impl serde::Serialize) {
        let mut seen = lock(&self.0);
        let seen = seen.entry(command_id.to_owned()).or_default();
        seen.runs += 1;
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

    /// How often the worker ran each command whose ID starts with `prefix`.
    fn runs(&self, prefix: &str) -> Vec<(String, u32)> {
        lock(&self.0)
            .iter()
            .filter(|(id, _)| id.starts_with(prefix))
            .map(|(id, seen)| (id.clone(), seen.runs))
            .collect()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One kernel with the scene's part of the bank on it: replaced whole on reset.
struct Run {
    app: AppHandle,
    /// The bank's client, which seals personal fields.
    client: Kernable,
    stage: Recorder,
    rows: bank::Rows,
    switches: bank::Switches,
    /// Where the kernel listens, for gRPC and for its own UI.
    grpc: String,
    ui: String,
    dir: PathBuf,
    /// What the scene's actions keep between steps, e.g. a second client.
    kept: Mutex<Kept>,
    // Dropped in this order: the kernel stops before its directory goes.
    kernel: Mutex<Process>,
    _dir: Dir,
}

#[derive(Default)]
struct Kept {
    mobile: Option<Kernable>,
    /// Milliseconds the first burst of a scene took.
    burst_ms: Option<f64>,
    /// Extra listeners the scene started, stopped with the run.
    listeners: Vec<kernable::ListenerHandle>,
}

impl Drop for Run {
    fn drop(&mut self) {
        self.app.stop();
        for listener in &lock(&self.kept).listeners {
            listener.stop();
        }
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

    /// Kills the kernel at once, like `kill -9`.
    fn kill(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
        if let Some(watchdog) = &mut self.1 {
            let _ = watchdog.kill();
            let _ = watchdog.wait();
        }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        self.kill();
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
    // Bound before the kernel starts, which takes a while: no one else takes the address meanwhile.
    let listener = tokio::net::TcpListener::bind(ui)
        .await
        .map_err(|e| format!("--ui-listen {ui}: {e}"))?;
    let base = std::env::temp_dir().join(format!("kernable-demo-{}", std::process::id()));
    std::fs::create_dir_all(&base)?;
    let demo = Arc::new(Demo {
        base,
        state: Mutex::default(),
    });
    let first = start(&demo, 1, &SCENES[0]).await;
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
        crate::http::serve_on(
            listener,
            true,
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
    async fn respond(self: &Arc<Self>, request: &[u8]) -> Answer {
        let text = String::from_utf8_lossy(request);
        let mut words = text.split_whitespace();
        let (method, target) = (words.next().unwrap_or(""), words.next().unwrap_or(""));
        let path = target.split('?').next().unwrap_or("");
        let asked = text
            .lines()
            .take_while(|line| !line.is_empty())
            .any(|line| line.to_ascii_lowercase().starts_with("x-kernable-demo:"));
        let bytes = match (method, path) {
            ("GET", "/") => reply("200 OK", "text/html; charset=utf-8", UI.as_bytes()),
            ("GET", "/demo") => reply("200 OK", "text/html; charset=utf-8", STAGE.as_bytes()),
            ("GET", "/api/demo") => json_reply(&self.view()),
            ("POST", _) if !asked => reply(
                "403 Forbidden",
                "text/plain",
                b"the demo takes requests from its Stage only\n",
            ),
            ("POST", "/api/demo/next") => self.next(),
            ("POST", "/api/demo/reset") => self.reset(),
            ("GET", _) => {
                let ui = lock(&self.state).run.as_ref().map(|r| r.ui.clone());
                match ui {
                    Some(ui) => return passed_on(ui, request.to_vec()),
                    None => reply("503 Service Unavailable", "text/plain", b"resetting\n"),
                }
            }
            _ => reply("404 Not Found", "text/plain", b"not found\n"),
        };
        bytes.into()
    }

    fn view(&self) -> Value {
        let state = lock(&self.state);
        let scene = &SCENES[0];
        let rows = match &state.run {
            Some(r) => json!({
                "accounts": r.rows.view("accounts"),
                "customers": r.rows.view("customers"),
                "people": r.rows.view("people"),
            }),
            None => json!({}),
        };
        let steps: Vec<Value> = scene
            .steps
            .iter()
            .enumerate()
            .map(|(i, s)| {
                let (sends, together, act) = match &s.does {
                    Does::Send { sends, together } => (
                        sends
                            .iter()
                            .map(|c| {
                                json!({
                                    "aggregate": c.aggregate,
                                    "id": c.id,
                                    "command": c.command,
                                    "who": c.who,
                                    "data": data(c),
                                })
                            })
                            .collect(),
                        *together,
                        false,
                    ),
                    Does::Act(_) => (Vec::new(), false, true),
                };
                let chapter = scene
                    .chapters
                    .iter()
                    .rposition(|c| c.from <= i)
                    .unwrap_or(0);
                json!({
                    "title": s.title,
                    "says": s.says,
                    "chapter": chapter,
                    "sends": sends,
                    "together": together,
                    "act": act,
                    "intents": intents(s.intents),
                })
            })
            .collect();
        let chapters: Vec<Value> = scene
            .chapters
            .iter()
            .map(|c| {
                json!({
                    "title": c.title,
                    "question": c.question,
                    "takeaway": c.takeaway,
                    "from": c.from,
                    "intents": intents(c.intents),
                })
            })
            .collect();
        let ids: Vec<Value> = scene
            .ids
            .iter()
            .map(|(aggregate, id)| json!({ "aggregate": aggregate, "id": id }))
            .collect();
        json!({
            "generation": state.generation,
            "ready": state.run.is_some(),
            "busy": state.busy,
            "error": state.error,
            "scene": {
                "title": scene.title,
                "intro": scene.intro,
                "ids": ids,
                "nodes": scene.nodes,
                "intents": intents(scene.intents),
                // The command and query types a middleware checks before the kernel acts.
                "checks": if scene.parts.rbac { vec!["Withdraw", "GetAccount"] } else { Vec::new() },
                "facts": scene.parts.payments,
                "chapters": chapters,
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
            if index >= SCENES[0].steps.len() {
                return conflict("the scene is over: reset to start again");
            }
            state.busy = Some("step");
            (run, &SCENES[0], index, state.generation)
        };
        let demo = Arc::clone(self);
        tokio::spawn(async move {
            let mut trace = step(&run, scene, generation, index).await;
            // What the step left, so the Stage can show it again later: the store's head and
            // every read model's rows.
            let head = ui_json(&run, "/api/state").await.ok();
            trace["head"] = head.map_or(Value::Null, |s| s["kernel"]["head"].clone());
            trace["rows"] = json!({
                "accounts": run.rows.view("accounts"),
                "customers": run.rows.view("customers"),
                "people": run.rows.view("people"),
            });
            let mut state = lock(&demo.state);
            if state.generation == generation {
                state.traces.push(trace);
                state.busy = None;
            }
        });
        json_reply(&json!({ "step": index }))
    }

    /// Replaces the kernel and the bank with new ones on a new store.
    fn reset(self: &Arc<Self>) -> Vec<u8> {
        let (generation, scene) = {
            let mut state = lock(&self.state);
            if state.busy == Some("reset") {
                return conflict("already resetting");
            }
            state.generation += 1;
            state.busy = Some("reset");
            state.traces.clear();
            state.error = None;
            // The old run stops here, or once a step still using it ends.
            state.run = None;
            (state.generation, &SCENES[0])
        };
        let demo = Arc::clone(self);
        tokio::spawn(async move {
            let run = start(&demo, generation, scene).await;
            demo.started(generation, run);
        });
        json_reply(&json!({ "generation": generation }))
    }

    fn started(&self, generation: u64, run: Result<Run, BoxError>) {
        let mut state = lock(&self.state);
        // A start a newer reset overtook: its run stops here.
        if generation < state.generation {
            return;
        }
        state.generation = generation;
        state.busy = None;
        match run {
            Ok(run) => state.run = Some(Arc::new(run)),
            Err(e) => state.error = Some(format!("the demo kernel did not start: {e}")),
        }
    }
}

/// `kernable start --dev` in `dir`, listening on `grpc` and serving its UI on `ui`; nothing
/// from a kernel configured in this shell.
fn kernel_command(dir: &Path, grpc: &str, ui: &str) -> Result<Command, BoxError> {
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args(["start", "--dev", "--listen", grpc, "--ui-listen", ui])
        .current_dir(dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(dir.join("kernel.log"))?,
        );
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("KERNABLE_") {
            command.env_remove(name);
        }
    }
    command.env("KERNABLE_LOG", "warn");
    Ok(command)
}

/// How often the demo starts its kernel on other addresses when it exits at once.
const KERNEL_STARTS: usize = 3;

/// Why the kernel did not start.
pub(super) enum NotStarted {
    /// It exited: another process may have taken one of its addresses; worth another try.
    Exited(String),
    Failed(BoxError),
}

impl NotStarted {
    pub(super) fn into_error(self) -> BoxError {
        match self {
            NotStarted::Exited(why) => why.into(),
            NotStarted::Failed(e) => e,
        }
    }
}

impl<E: Into<BoxError>> From<E> for NotStarted {
    fn from(e: E) -> Self {
        NotStarted::Failed(e.into())
    }
}

/// Starts the kernel in `dir` and waits until it listens on `grpc` and `ui`.
async fn start_kernel(dir: &Path, grpc: &str, ui: &str) -> Result<Process, NotStarted> {
    let mut kernel = Process::spawn(&mut kernel_command(dir, grpc, ui)?)?;
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        // Checked first: what answers on an address may be another process that took it.
        if let Some(status) = kernel.0.try_wait()? {
            let log = std::fs::read_to_string(dir.join("kernel.log")).unwrap_or_default();
            return Err(NotStarted::Exited(format!(
                "the kernel exited ({status}): {}",
                log.trim()
            )));
        }
        if TcpStream::connect(grpc).await.is_ok() && TcpStream::connect(ui).await.is_ok() {
            return Ok(kernel);
        }
        if Instant::now() > deadline {
            return Err("the kernel did not listen within 30 s".into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Starts a kernel on a new store in its own directory, and the scene's bank on it.
async fn start(demo: &Demo, generation: u64, scene: &Scene) -> Result<Run, BoxError> {
    let dir = Dir(demo.base.join(format!("run-{generation}")));
    std::fs::create_dir_all(&dir.0)?;
    // The addresses are free when picked, but another process may take one before the kernel
    // binds it: then the kernel exits, and starts again on others.
    let mut tries = 0;
    let (kernel, grpc, ui) = loop {
        let (grpc, ui) = (free_address()?, free_address()?);
        match start_kernel(&dir.0, &grpc, &ui).await {
            Ok(kernel) => break (kernel, grpc, ui),
            Err(NotStarted::Exited(why)) if tries + 1 < KERNEL_STARTS => {
                tries += 1;
                tracing::warn!("{why}; starting it again on other addresses");
                for file in std::fs::read_dir(&dir.0)? {
                    let path = file?.path();
                    let _ = if path.is_dir() {
                        std::fs::remove_dir_all(&path)
                    } else {
                        std::fs::remove_file(&path)
                    };
                }
            }
            Err(e) => return Err(e.into_error()),
        }
    };
    let (stage, rows, switches) = (
        Recorder::default(),
        bank::Rows::default(),
        bank::Switches::default(),
    );
    let connection = Kernable::new(
        Options::new(dir.0.join("app.key"))
            .url(format!("http://{grpc}"))
            .service("demo"),
    )?;
    let bank = bank::bank(scene.parts, &stage, &rows, &switches);
    let client = connection.with_domain(&bank);
    let app = connection.app(
        App::new()
            .service("demo")
            .description("the bank of kernable demo")
            .domain(&bank),
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
        switches,
        grpc,
        ui,
        dir: dir.0.clone(),
        kept: Mutex::default(),
        kernel: Mutex::new(kernel),
        _dir: dir,
    })
}

/// Runs step `index` of `scene` and says what happened.
async fn step(run: &Arc<Run>, scene: &'static Scene, generation: u64, index: usize) -> Value {
    let s = &scene.steps[index];
    match &s.does {
        Does::Act(act) => {
            let prefix = format!("demo-{generation}-{}", index + 1);
            match act::run(run, *act, &prefix).await {
                Ok(mut trace) => {
                    trace["step"] = json!(index);
                    trace["kind"] = json!("act");
                    trace
                }
                Err(e) => json!({
                    "step": index,
                    "kind": "act",
                    "beats": [act::beat(&format!("This step failed: {e}"), &[])],
                    "commands": [],
                }),
            }
        }
        Does::Send { sends, together } => {
            let mut results = Vec::new();
            if *together {
                let mut tasks = Vec::new();
                for (n, c) in sends.iter().enumerate() {
                    let run = Arc::clone(run);
                    let command_id = format!("demo-{generation}-{}-{}", index + 1, n + 1);
                    tasks.push(tokio::spawn(async move { sent(&run, c, command_id).await }));
                }
                for task in tasks {
                    results.push(task.await.unwrap_or_else(
                        |e| json!({ "outcome": "failed", "error": e.to_string() }),
                    ));
                }
            } else {
                for (n, c) in sends.iter().enumerate() {
                    let command_id = format!("demo-{generation}-{}-{}", index + 1, n + 1);
                    results.push(sent(run, c, command_id).await);
                }
            }
            json!({ "step": index, "kind": "sends", "sends": results })
        }
    }
}

/// Sends one command and says what happened, with the time each point took from the submit,
/// in microseconds.
async fn sent(run: &Run, c: &Send, command_id: String) -> Value {
    let data = data(c);
    let sent_at = Instant::now();
    let (_, query) = scenes::read_side(c.aggregate);
    let sent = bank::send(
        &run.client,
        &command_id,
        (c.aggregate, c.id, c.command),
        data.clone(),
        (query, c.who),
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
        "aggregate": c.aggregate,
        "id": c.id,
        "who": c.who,
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

/// Intentions as the Stage reads them: `{ "worker": "Open an account only once." }`.
fn intents(intents: scenes::Intents) -> Value {
    Value::Object(
        intents
            .iter()
            .map(|(node, text)| ((*node).to_owned(), json!(text)))
            .collect(),
    )
}

/// Passes a request on to the kernel's UI, its answer streamed back as it comes: `/api/watch`
/// never ends.
fn passed_on(ui: String, request: Vec<u8>) -> Answer {
    Answer::Stream(Box::new(move |mut conn| {
        Box::pin(async move {
            let connected = async {
                let mut upstream = TcpStream::connect(&ui).await?;
                upstream.write_all(&request).await?;
                Ok::<_, std::io::Error>(upstream)
            };
            match tokio::time::timeout(PROXY_TIMEOUT, connected).await {
                Ok(Ok(mut upstream)) => {
                    // Until either side closes.
                    let _ = tokio::io::copy(&mut upstream, &mut conn).await;
                }
                _ => {
                    let failed = reply(
                        "502 Bad Gateway",
                        "text/plain",
                        b"the kernel's UI did not answer\n",
                    );
                    let _ = conn.write_all(&failed).await;
                }
            }
        })
    }))
}

/// Passes a request on to the kernel's UI and returns its whole answer.
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

/// `GET path` from the kernel's UI, as JSON.
async fn ui_json(run: &Run, path: &str) -> Result<Value, BoxError> {
    let request = format!("GET {path} HTTP/1.1\r\nHost: {}\r\n\r\n", run.ui);
    let response = proxy(&run.ui, request.as_bytes()).await;
    let text = String::from_utf8_lossy(&response);
    let (head, body) = text.split_once("\r\n\r\n").ok_or("no answer from the UI")?;
    if !head.starts_with("HTTP/1.1 200") {
        return Err(format!("the UI answered {}", head.lines().next().unwrap_or("")).into());
    }
    Ok(serde_json::from_str(body)?)
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

    /// The nodes the flow diagram draws in every scene; a scene adds its `nodes`.
    const NODES: &[&str] = &["client", "kernel", "worker", "store", "model"];

    #[test]
    fn every_intention_belongs_to_a_node_the_scene_shows() {
        for scene in SCENES {
            let shown = |node: &str| NODES.contains(&node) || scene.nodes.contains(&node);
            let all = scene
                .intents
                .iter()
                .chain(scene.chapters.iter().flat_map(|c| c.intents))
                .chain(scene.steps.iter().flat_map(|s| s.intents));
            for (node, text) in all {
                assert!(shown(node), "{}: {node}", scene.title);
                assert!(
                    text.ends_with('.') && text.len() < 90,
                    "{}: {text}",
                    scene.title
                );
            }
        }
    }

    #[test]
    fn every_step_sends_json() {
        for step in SCENES.iter().flat_map(|s| s.steps) {
            if let Does::Send { sends, .. } = &step.does {
                for c in *sends {
                    serde_json::from_str::<Value>(c.data).unwrap();
                }
            }
        }
    }

    #[test]
    fn the_chapters_cover_the_steps_in_order() {
        for scene in SCENES {
            let froms: Vec<usize> = scene.chapters.iter().map(|c| c.from).collect();
            assert_eq!(froms[0], 0);
            assert!(froms.windows(2).all(|w| w[0] < w[1]), "{froms:?}");
            assert!(froms.iter().all(|&f| f < scene.steps.len()));
            for step in scene.steps {
                if let Does::Send { sends, .. } = &step.does {
                    for c in *sends {
                        assert!(scene.ids.contains(&(c.aggregate, c.id)), "{}", c.id);
                    }
                }
            }
        }
    }
}
