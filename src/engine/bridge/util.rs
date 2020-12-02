use anyhow::anyhow;
use anyhow::Error;
use ethabi::Token as EthTokenValue;
use ethabi::{ParamType};
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

pub fn map_eth_ton(eth: EthTokenValue) -> TonTokenValue {
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
            let number = BigInt::from_signed_bytes_be(&bytes);
            TonTokenValue::Int(ton_abi::Int { number, size: 256 }) //fixme ? check correctness
        }
        EthTokenValue::Address(ad) => TonTokenValue::Bytes(ad.0.to_vec()),
        EthTokenValue::String(a) => TonTokenValue::Bytes(Vec::from(a)),
        _ => unimplemented!(),
    }
}

pub fn map_ton_eth(ton: TonTokenValue) -> EthTokenValue {
    match ton {
        TonTokenValue::Uint(a) => {
            let bytes = a.number.to_bytes_le();
            EthTokenValue::Uint(ethabi::Uint::from_little_endian(&bytes))
        }
        TonTokenValue::Int(a) => {
            let mut bytes = a.number.to_signed_bytes_le();
            let sign = bytes
                .last()
                .map(|first| (first >> 7) * 255)
                .unwrap_or_default();
            bytes.resize(32, sign);

            EthTokenValue::Int(ethabi::Int::from_little_endian(&bytes))
        }
        TonTokenValue::Bytes(a) => EthTokenValue::Bytes(a),
        TonTokenValue::Address(a) => EthTokenValue::String(a.to_string()),
        TonTokenValue::FixedBytes(a) => EthTokenValue::FixedBytes(a),
        TonTokenValue::Bool(a) => EthTokenValue::Bool(a),
        TonTokenValue::Cell(a) => EthTokenValue::Bytes(a.data().to_vec()),
        _ => unimplemented!(),
    }
}

///returns signature and its hash.
pub fn abi_to_topic_hash(abi: &str) -> Result<Vec<(H256, Vec<ParamType>)>, Error> {
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
    let abi_list: Vec<Abi> = serde_json::from_str(abi)?;
    let abi_list = abi_list.into_iter().filter(|x| x.type_field == "event");
    let mut result = Vec::new();
    for abi in abi_list {
        let fn_name = abi.name;
        let input_types: String = abi
            .inputs
            .iter()
            .map(|x| x.type_field.clone())
            .collect::<Vec<String>>()
            .join(",");
        let abi_types: Result<Vec<ParamType>, Error> = abi
            .inputs
            .iter()
            .map(|x| from_str(x.internal_type.as_str()))
            .collect();
        let signature = format!("{}({})", fn_name, input_types);
        result.push((
            H256::from_slice(&*Keccak256::digest(signature.as_bytes())),
            abi_types?,
        ))
    }
    Ok(result)
}

#[cfg(test)]
mod test {
    use ethabi::ParamType;
    use ethabi::Token as EthTokenValue;
    use num_bigint::BigInt;
    use sha3::Digest;
    use sha3::Keccak256;
    use ton_abi::TokenValue as TonTokenValue;

    use relay_eth::ws::H256;

    use crate::engine::bridge::util::{abi_to_topic_hash, from_str, map_eth_ton, map_ton_eth};

    const ABI: &str = r#"
[
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
  },
  {
    "inputs": [],
    "name": "currentState",
    "outputs": [
      {
        "internalType": "uint256",
        "name": "",
        "type": "uint256"
      }
    ],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [
      {
        "internalType": "uint256",
        "name": "newState",
        "type": "uint256"
      }
    ],
    "name": "setState",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  }
]
  "#;

    #[test]
    fn test_event_contract_abi() {
        let hash: Vec<_> = abi_to_topic_hash(ABI)
            .unwrap()
            .into_iter()
            .map(|x| x.0)
            .collect();
        let expected = vec![H256::from_slice(&*Keccak256::digest(
            b"StateChange(uint256,address)",
        ))];
        assert_eq!(expected, hash);
    }
    //todo test abi parsing

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

    fn make_int256_le(number: i64) -> [u8; 32] {
        let value = BigInt::from(number);
        let mut value_bytes = value.to_signed_bytes_le();

        let sign = value_bytes
            .last()
            .map(|first| (first >> 7) * 255)
            .unwrap_or_default();
        value_bytes.resize(32, sign);

        let mut result = [sign; 32];
        result[0..value_bytes.len()].clone_from_slice(&value_bytes);
        result
    }

    #[test]
    fn test_conversion_int() {
        use ethabi::Int as EInt;
        use ton_abi::Int as TInt;

        let number = make_int256_le(-1234567);

        let eth = EthTokenValue::Int(EInt::from_little_endian(&number));
        let ton_expected = TonTokenValue::Int(TInt {
            number: BigInt::from_signed_bytes_le(&number),
            size: 256,
        });
        assert_eq!(map_eth_ton(eth), ton_expected);
    }

    #[test]
    fn test_conversion_int_plus() {
        use ethabi::Int as EInt;
        use ton_abi::Int as TInt;

        let number = make_int256_le(1234567);

        let eth = EthTokenValue::Int(EInt::from_little_endian(&number));
        let ton_expected = TonTokenValue::Int(TInt {
            number: BigInt::from_signed_bytes_le(&number),
            size: 256,
        });
        assert_eq!(map_eth_ton(eth), ton_expected);
    }

    #[test]
    fn ton_test_conversion_int_plus() {
        use ethabi::Int as EInt;
        use ton_abi::Int as TInt;

        let number = make_int256_le(1234567);

        let eth_expected = EthTokenValue::Int(EInt::from_little_endian(&number));
        let ton = TonTokenValue::Int(TInt {
            number: BigInt::from_signed_bytes_le(&number),
            size: 256,
        });
        assert_eq!(map_ton_eth(ton), eth_expected);
    }

    #[test]
    fn ton_test_conversion_int() {
        use ethabi::Int as EInt;
        use ton_abi::Int as TInt;

        let number = make_int256_le(-1234567);

        let eth = EthTokenValue::Int(EInt::from_little_endian(&number));
        let ton_expected = TonTokenValue::Int(TInt {
            number: BigInt::from_signed_bytes_le(&number),
            size: 256,
        });
        let got = map_ton_eth(ton_expected);
        assert_eq!(got, eth);
    }

    #[test]
    fn ton_test_conversion_uint() {
        use ethabi::Uint as EUint;
        use ton_abi::Uint as TUint;
        let eth = EthTokenValue::Uint(EUint::from(1234567));
        let ton_expected = TonTokenValue::Uint(TUint::new(1234567, 256));
        assert_eq!(map_ton_eth(ton_expected), eth);
    }
}
