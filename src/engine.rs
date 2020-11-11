use crate::config::RelayConfig;
use anyhow::Error;
use relay_eth::ws::EthConfig;
use url::Url;

use tokio::time::Duration;


async fn serve()
{

}

pub async fn run(config: RelayConfig) -> Result<(), Error> {
    let _eth_config = EthConfig::new(Url::parse(config.eth_node_address.as_str())?).await;
    loop {
        tokio::time::delay_for(Duration::from_secs(1)).await;
    }
    Ok(())
}

