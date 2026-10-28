//! The worker: runs the shop's commands.
use kernable::{BoxError, Kernable, Options};
use shop::shop;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let shop = shop();
    // #region worker
    let kernable = Kernable::new(Options::default())?;
    kernable
        .worker(&shop.customer_aggregate)
        .aggregate(&shop.order_aggregate)
        .run()
        .await;
    // #endregion worker
    Ok(())
}
