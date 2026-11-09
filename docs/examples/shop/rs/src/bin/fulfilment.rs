//! Registers the fulfilment process with the kernel: once per deploy, after the worker is up.
use kernable::{BoxError, Kernable, Options};
use shop::shop;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let fulfilment = shop().fulfilment;
    // #region register
    let kernable = Kernable::new(Options::default())?;
    let registered = kernable.register_process(&fulfilment).await?;
    let unchanged = if registered.new { "" } else { " (unchanged)" };
    println!(
        "{} version {}{unchanged}",
        registered.name, registered.version
    );
    // #endregion register
    Ok(())
}
