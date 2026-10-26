use std::{path::Path, process::ExitCode};

use kernable_kernel::{BoxError, Config, Kernel, serve};

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
    serve(Kernel::open(config)?).await
}
