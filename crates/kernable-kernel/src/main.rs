use std::{path::Path, process::ExitCode, sync::Arc};

use kernable_kernel::{BoxError, Config, Kernel, serve};
use tokio::signal::unix::{SignalKind, signal};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    // KERNABLE_LOG sets the level filter (default `info`); KERNABLE_LOG_FORMAT=json logs JSON.
    let filter = EnvFilter::try_from_env("KERNABLE_LOG").unwrap_or_else(|_| EnvFilter::new("info"));
    let logs = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
    if std::env::var("KERNABLE_LOG_FORMAT").as_deref() == Ok("json") {
        logs.json().init();
    } else {
        logs.init();
    }

    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: kernable-kernel <kernable.toml>");
        return ExitCode::from(2);
    };
    match run(Path::new(&path)).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %e, "kernel stopped");
            ExitCode::FAILURE
        }
    }
}

async fn run(path: &Path) -> Result<(), BoxError> {
    let config = Config::load(path)?;
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        listen = %config.listen,
        tls = config.tls.is_some(),
        "kernel starting"
    );
    let kernel = Kernel::open(config)?;
    let mut server = tokio::spawn(serve(Arc::clone(&kernel)));
    tokio::select! {
        result = &mut server => result??,
        signal = shutdown_signal() => {
            // Workers and listeners stay connected, so running commands can finish and settle.
            tracing::info!(signal = signal?, "shutting down: finishing running requests");
            if !kernel.drain().await {
                tracing::warn!("drain timed out: unfinished commands run again after a restart");
            }
            server.abort();
        }
    }
    kernel.shutdown()
}

async fn shutdown_signal() -> std::io::Result<&'static str> {
    let mut term = signal(SignalKind::terminate())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => Ok("SIGINT"),
        _ = term.recv() => Ok("SIGTERM"),
    }
}
