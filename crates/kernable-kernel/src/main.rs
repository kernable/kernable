use std::{path::Path, process::ExitCode, sync::Arc};

use kernable_kernel::{BoxError, Config, Kernel, serve};
use tokio::signal::unix::{SignalKind, signal};

#[tokio::main]
async fn main() -> ExitCode {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: kernable-kernel <kernable.toml>");
        return ExitCode::from(2);
    };
    match run(Path::new(&path)).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(path: &Path) -> Result<(), BoxError> {
    let config = Config::load(path)?;
    eprintln!(
        "kernable kernel {} listening on {}",
        env!("CARGO_PKG_VERSION"),
        config.listen
    );
    let kernel = Kernel::open(config)?;
    tokio::select! {
        result = serve(Arc::clone(&kernel)) => result?,
        signal = shutdown_signal() => {
            eprintln!("received {}, shutting down", signal?);
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
