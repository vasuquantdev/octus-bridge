use failure::Fallible;

#[tokio::main]
async fn main() -> Fallible<()> {
    env_logger::init();

    log::info!("Relay ready.");

    futures::future::pending().await
}
