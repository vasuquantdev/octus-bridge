use anyhow::{Context, Result};
use ethabi::Contract;
use serde_json::Value;

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
    use super::*;

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
        let expected = web3::signing::keccak256(b"StateChange(uint256,address)");
        assert_eq!(expected, get_topic_hash(ABI).unwrap());
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
