use anyhow::Result;
use serde::Deserialize;

pub fn decode_eth_event_abi(abi: &str) -> Result<ethabi::Event> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    pub enum Operation {
        Event(ethabi::Event),
    }

    serde_json::from_str::<Operation>(&abi)
        .map(|item| match item {
            Operation::Event(event) => event,
        })
        .map_err(anyhow::Error::from)
}

pub fn get_topic_hash(event: &ethabi::Event) -> [u8; 32] {
    event.signature().to_fixed_bytes()
}

#[cfg(test)]
mod test {
    use super::*;

    const ABI: &str = r#"{
    "anonymous": false,
    "inputs": [
        {
            "indexed": false,
            "name": "state",
            "type": "uint256"
        },
        {
            "indexed": false,
            "name": "author",
            "type": "address"
        }
    ],
    "outputs": [],
    "name": "StateChange"
}"#;

    const ABI2: &str = r#"{
    "inputs": [
        {
            "name": "a",
            "type": "address",
            "indexed": true
        },
        {
            "components": [
                {
                    "internalType": "address",
                    "name": "to",
                    "type": "address"
                },
                {
                    "internalType": "uint256",
                    "name": "value",
                    "type": "uint256"
                },
                {
                    "internalType": "bytes",
                    "name": "data",
                    "type": "bytes"
                }
            ],
            "indexed": false,
            "internalType": "struct Action[]",
            "name": "b",
            "type": "tuple[]"
        }
    ],
    "name": "E",
    "outputs": [],
    "anonymous": false
}"#;

    const ABI3: &str = r#"{
    "name": "TokenLock",
    "anonymous": false,
    "inputs": [
        {
            "name": "amount",
            "type": "uint128"
        },
        {
            "name": "wid",
            "type": "int8"
        },
        {
            "name": "addr",
            "type": "uint256"
        },
        {
            "name": "pubkey",
            "type": "uint256"
        }
    ],
    "outputs": []
}"#;

    #[test]
    fn test_bad_abi() {
        assert!(decode_eth_event_abi("lol").is_err());
    }

    #[test]
    fn test_abi() {
        let expected = web3::signing::keccak256(b"StateChange(uint256,address)");
        let event = decode_eth_event_abi(ABI).unwrap();
        assert_eq!(expected, get_topic_hash(&event));
    }

    #[test]
    fn test_abi2() {
        decode_eth_event_abi(ABI2).unwrap();
    }

    #[test]
    fn test_abi3() {
        decode_eth_event_abi(ABI3).unwrap();
    }
}
