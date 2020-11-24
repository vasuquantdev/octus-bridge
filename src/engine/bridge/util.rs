use anyhow::anyhow;
use anyhow::Error;
use ethabi::ParamType;
use serde::{Deserialize, Serialize};
use sha3::digest::Digest;
use sha3::Keccak256;

use relay_eth::ws::H256;

pub fn from_str(token: &str) -> Result<ParamType, Error> {
    Ok(match token.to_lowercase().as_str() {
        str if str.starts_with("uint") => {
            let num = str.trim_start_matches(char::is_alphabetic).parse()?;
            if num % 8 != 0 {
                return Err(anyhow!("Bad int size: {}", num));
            }
            ParamType::Uint(num)
        }
        str if str.starts_with("int") => {
            let num = str.trim_start_matches(char::is_alphabetic).parse()?;
            if num % 8 != 0 {
                return Err(anyhow!("Bad uint size: {}", num));
            }
            ParamType::Int(num)
        }
        str if str.starts_with("address") => ParamType::Address,
        str if str.starts_with("bool") => ParamType::Bool,
        str if str.starts_with("string") => ParamType::String,
        str if str.starts_with("bytes") => {
            let num = str.trim_start_matches(char::is_alphabetic).parse()?;
            ParamType::FixedBytes(num)
        }
        _ => unimplemented!(),
    })
}

pub fn abi_to_topic_hash(abi: &str) -> Result<H256, Error> {
    //todo list of hashes?
    #[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Abi {
        pub anonymous: Option<bool>,
        pub inputs: Vec<Input>,
        pub name: String,
        #[serde(rename = "type")]
        pub type_field: String,
        #[serde(default)]
        pub outputs: Vec<Output>,
        pub state_mutability: Option<String>,
    }

    #[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Input {
        pub indexed: Option<bool>,
        pub internal_type: String,
        pub name: String,
        #[serde(rename = "type")]
        pub type_field: String,
    }

    #[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Output {
        pub internal_type: String,
        pub name: String,
        #[serde(rename = "type")]
        pub type_field: String,
    }
    let abi: Abi = serde_json::from_str(abi)?;
    let fn_name = abi.name;
    let input_types: String = abi
        .inputs
        .into_iter()
        .map(|x| x.type_field)
        .collect::<Vec<String>>()
        .join(",");
    // let output_type =abi.outputs.into_iter().map(|x|x.type_field).collect::<Vec<String>>().first(); //todo ????
    let signature = format!("{}({})", fn_name, input_types);
    Ok(H256::from_slice(&*Keccak256::digest(signature.as_bytes())))
}

// fn serialize_eth_payload_in_ton(data: &[u8]) -> Vec<u8> {}

#[cfg(test)]
mod test {
    use ethabi::ParamType;

    use sha3::Digest;
    use sha3::Keccak256;

    use relay_eth::ws::H256;

    use crate::engine::bridge::util::{abi_to_topic_hash, from_str};

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
    "type": "event"
  }"#;

    #[test]
    fn test_event_contract_abi() {
        let hash = abi_to_topic_hash(ABI).unwrap();
        let expected = H256::from_slice(&*Keccak256::digest(b"StateChange(uint256,address)"));
        assert_eq!(expected, hash);
    }

    #[test]
    fn test_u256() {
        let expected = ParamType::Uint(256);
        let got = from_str("uint256").unwrap();
        assert_eq!(expected, got);
    }
    #[test]
    fn test_i64() {
        let expected = ParamType::Int(64);
        let got = from_str("Int64").unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn test_bytes() {
        let expected = ParamType::FixedBytes(32);
        let got = from_str("bytes32").unwrap();
        assert_eq!(expected, got);
    }
    #[test]
    fn test_addr() {
        let expected = ParamType::Address;
        let got = from_str("address").unwrap();
        assert_eq!(expected, got);
    }
}
