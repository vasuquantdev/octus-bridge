use anyhow::Result;
use web3::api::Eth;
use web3::contract::Contract;
use web3::Transport;

pub fn staking_contract<T>(eth: Eth<T>, address: ethabi::Address) -> Result<Contract<T>>
where
    T: Transport,
{
    let json = include_bytes!("StakingRelayVerifier.abi");
    Ok(web3::contract::Contract::from_json(eth, address, json)?)
}
