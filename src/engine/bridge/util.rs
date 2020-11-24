use anyhow::anyhow;
use anyhow::Error;
use ethabi::ParamType;
use ethabi::Token as EthTokenValue;
use num_bigint::{BigInt, BigUint};
use serde::{Deserialize, Serialize};
use sha3::digest::Digest;
use sha3::Keccak256;
use ton_abi::TokenValue as TonTokenValue;

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

fn map_eth_ton(eth: EthTokenValue) -> TonTokenValue {
    match eth {
        EthTokenValue::FixedBytes(x) => TonTokenValue::FixedBytes(x.to_vec()),
        EthTokenValue::Bytes(x) => TonTokenValue::Bytes(x.to_vec()),
        EthTokenValue::Uint(x) => {
            let mut bytes = [0u8; 256 / 8];
            x.to_big_endian(&mut bytes);
            let number = BigUint::from_bytes_be(&bytes);
            TonTokenValue::Uint(ton_abi::Uint { number, size: 256 }) //fixme ? check correctness
        }
        EthTokenValue::Int(x) => {
            let mut bytes = [0u8; 256 / 8];
            x.to_big_endian(&mut bytes);

            let sign = bytes[0] & 0x80;

            let sign = if sign == 1 {
                num_bigint::Sign::Plus
            } else {
                num_bigint::Sign::Minus
            };
            dbg!(&sign);
            let number = BigInt::from_signed_bytes_be(&bytes);
            TonTokenValue::Int(ton_abi::Int { number, size: 256 }) //fixme ? check correctness
        }
        // TonTokenValue::Uint(x.as_byte_slice()) }
        _ => unimplemented!(),
    }
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
    use ethabi::Token as EthTokenValue;
    use sha3::Digest;
    use sha3::Keccak256;
    use ton_abi::TokenValue as TonTokenValue;

    use relay_eth::ws::H256;

    use crate::engine::bridge::util::{abi_to_topic_hash, from_str, map_eth_ton};

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

    #[test]
    fn test_conversion_uint() {
        use ethabi::Uint as EUint;
        use ton_abi::Uint as TUint;
        let eth = EthTokenValue::Uint(EUint::from(1234567));
        let ton_expected = TonTokenValue::Uint(TUint::new(1234567, 256));
        assert_eq!(map_eth_ton(eth), ton_expected);
    }

    #[test]
    fn test_conversion_int() {
        use ethabi::Int as EInt;
        use num_bigint::BigInt;
        use ton_abi::Int as TInt;

        let value = BigInt::from(-1234567i64);
        let mut value_bytes = value.to_signed_bytes_le();

        let sign = value_bytes
            .last()
            .map(|first| (first >> 7) * 255)
            .unwrap_or_default();
        value_bytes.resize(32, sign);

        let eth = EthTokenValue::Int(EInt::from_little_endian(&value_bytes));
        let ton_expected = TonTokenValue::Int(TInt {
            number: BigInt::from_signed_bytes_le(&value_bytes),
            size: 256,
        });
        assert_eq!(map_eth_ton(eth), ton_expected);
    }

    #[test]
    fn test_conversion_int_plus() {
        use ethabi::Int as EInt;
        use ton_abi::Int as TInt;
        let eth = EthTokenValue::Int(EInt::from(1234567));
        let ton_expected = TonTokenValue::Int(TInt::new(1234567, 256));
        assert_eq!(map_eth_ton(eth), ton_expected);
    }
}
