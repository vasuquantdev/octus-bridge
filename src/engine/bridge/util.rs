use anyhow::Error;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::from_value;
use serde_json::Value;
use sha3::digest::Digest;
use sha3::Keccak256;

use relay_eth::ws::H256;

pub fn abi_to_topic_hash(abi: Value) -> Result<H256, Error> { //todo list of hashes?
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
    let abi: Abi = from_value(abi)?;
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

#[cfg(test)]
mod test {
    use hex::encode;
    use serde_json::Value;
    use sha3::Digest;
    use sha3::Keccak256;

    use relay_eth::ws::H256;

    use crate::engine::bridge::util::abi_to_topic_hash;

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
        let value: Value = serde_json::from_str(ABI).unwrap();
        let hash = abi_to_topic_hash(value).unwrap();
        let expected = H256::from_slice(&*Keccak256::digest(b"StateChange(uint256,address)"));
        assert_eq!(expected, hash);
    }
}
