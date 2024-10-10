#[cfg(not(feature = "disable-staking"))]
pub fn staking_contract<T>(
    eth: web3::api::Eth<T>,
    address: ethabi::Address,
) -> anyhow::Result<web3::contract::Contract<T>>
where
    T: web3::Transport,
{
    let json = include_bytes!("StakingRelayVerifier.abi");
    Ok(web3::contract::Contract::from_json(eth, address, json)?)
}
