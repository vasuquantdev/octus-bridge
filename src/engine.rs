use crate::config::RelayConfig;
use anyhow::Error;
use relay_eth::ws::EthConfig;
use url::Url;
use warp::Filter;


async fn serve()
{

}

pub async fn run(config: RelayConfig) -> Result<(), Error> {
    let eth_config = EthConfig::new(Url::parse(config.eth_node_address.as_str())?).await;

    Ok(())
}

