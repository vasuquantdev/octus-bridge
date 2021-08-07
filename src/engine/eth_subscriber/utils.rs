use anyhow::Context;
use anyhow::Result;
use ethabi::{Contract, Event};
use serde_json::Value;
use std::convert::TryInto;
use std::time::Duration;
use tryhard::backoff_strategies::{ExponentialBackoff, FixedBackoff};
use tryhard::{NoOnRetry, RetryFutureConfig};

#[inline]
fn generate_default_timeout_config(
    total_time: Duration,
) -> RetryFutureConfig<ExponentialBackoff, NoOnRetry> {
    let max_delay = Duration::from_secs(600);
    let times = crate::utils::calculate_times_from_max_delay(
        Duration::from_secs(1),
        2f64,
        max_delay,
        total_time,
    );
    tryhard::RetryFutureConfig::new(times)
        .exponential_backoff(Duration::from_secs(1))
        .max_delay(Duration::from_secs(600))
}

fn generate_fixed_config(
    sleep_time: Duration,
    total_time: Duration,
) -> RetryFutureConfig<FixedBackoff, NoOnRetry> {
    let times = (total_time.as_secs() / sleep_time.as_secs())
        .try_into()
        .expect("Overflow");
    tryhard::RetryFutureConfig::new(times).fixed_backoff(sleep_time)
}

pub fn get_topic_hash(abi: &str) -> Result<[u8; 32]> {
    let abi: Value = serde_json::from_str(abi).context("Bad value")?;
    let json = serde_json::json!([abi,]);
    let contract: Contract = serde_json::from_value(json)?;
    let event = contract
        .events
        .values()
        .next()
        .context("No event provided")?
        .first()
        .context("No event provided")?;
    Ok(event.signature().to_fixed_bytes())
}

#[cfg(test)]
mod test {
    use crate::engine::eth_subscriber::utils::get_topic_hash;

    const ABI: &str = r#"
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": false,
        "internalType": "uint256",
        "name": "state",
        "type": "uint256"
      },
      {
        "indexed": false,
        "internalType": "address",
        "name": "author",
        "type": "address"
      }
    ],
    "name": "StateChange",
    "type":"event"
  }
  "#;
    const ABI2: &str = r#"
  {
      "anonymous":false,
      "inputs":[
         {
            "indexed":false,
            "internalType":"uint256",
            "name":"state",
            "type":"uint256"
         }
      ],
      "name":"EthereumStateChange",
      "type":"event"
   }
  "#;
    const ABI3: &str = r#"
  {
   "name":"TokenLock",
    "type":"event",
    "anonymous":false,
   "inputs":[
      {
         "name":"amount",
         "type":"uint128"
      },
      {
         "name":"wid",
         "type":"int8"
      },
      {
         "name":"addr",
         "type":"uint256"
      },
      {
         "name":"pubkey",
         "type":"uint256"
      }
   ],
   "outputs":[
      
   ]
}
  "#;

    #[test]
    fn test_bad_abi() {
        let res = get_topic_hash("lol").is_err();
        assert!(res);
    }

    #[test]
    fn test_abi() {
        get_topic_hash(ABI).unwrap();
    }

    #[test]
    fn test_abi2() {
        get_topic_hash(ABI2).unwrap();
    }

    #[test]
    fn test_abi3() {
        get_topic_hash(ABI3).unwrap();
    }
}
