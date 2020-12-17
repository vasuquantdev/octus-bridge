use anyhow::anyhow;
use anyhow::Error;
use ethabi::{ParamType as EthParamType, ParamType, Token as EthTokenValue};
use num_bigint::{BigInt, BigUint};
use serde::{Deserialize, Serialize};
use sha3::digest::Digest;
use sha3::Keccak256;
use ton_abi::{ParamType as TonParamType, TokenValue as TonTokenValue, TokenValue};

use relay_eth::ws::H256;
use relay_ton::contracts::EthereumEventConfiguration;
use relay_ton::prelude::Cell;

/// Returns topic hash and abi for ETH and TON
pub fn parse_eth_abi(abi: &str) -> Result<(H256, Vec<EthParamType>, Vec<TonParamType>), Error> {
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
        .iter()
        .map(|x| x.type_field.clone())
        .collect::<Vec<String>>()
        .join(",");

    let eth_abi_params = abi
        .inputs
        .iter()
        .map(|x| eth_param_from_str(x.internal_type.as_str()))
        .collect::<Result<Vec<_>, Error>>()?;

    let ton_abi_params = map_eth_abi(&eth_abi_params)?;

    let signature = format!("{}({})", fn_name, input_types);
    Ok((
        H256::from_slice(&*Keccak256::digest(signature.as_bytes())),
        eth_abi_params,
        ton_abi_params,
    ))
}

pub fn validate_ethereum_event_configuration(
    config: &EthereumEventConfiguration,
) -> Result<(), Error> {
    let EthereumEventConfiguration {
        ethereum_event_abi, ..
    } = config;
    serde_json::from_str::<serde_json::Value>(&ethereum_event_abi)
        .map_err(|e| Error::new(e).context("Bad abi"))?;
    Ok(())
}

pub fn eth_param_from_str(token: &str) -> Result<EthParamType, Error> {
    Ok(match token.to_lowercase().as_str() {
        str if str.starts_with("uint") => {
            let num = str.trim_start_matches(char::is_alphabetic).parse()?;
            if num % 8 != 0 {
                return Err(anyhow!("Bad int size: {}", num));
            }
            EthParamType::Uint(num)
        }
        str if str.starts_with("int") => {
            let num = str.trim_start_matches(char::is_alphabetic).parse()?;
            if num % 8 != 0 {
                return Err(anyhow!("Bad uint size: {}", num));
            }
            EthParamType::Int(num)
        }
        str if str.starts_with("address") => EthParamType::Address,
        str if str.starts_with("bool") => EthParamType::Bool,
        str if str.starts_with("string") => EthParamType::String,
        str if str.starts_with("bytes") => {
            let num = str.trim_start_matches(char::is_alphabetic).parse()?;
            EthParamType::FixedBytes(num)
        }
        _ => unimplemented!(),
    })
}

pub fn parse_ton_event_data(
    eth_abi: &[EthParamType],
    ton_abi: &[TonParamType],
    data: Cell,
) -> Result<Vec<EthTokenValue>, Error> {
    if eth_abi.len() != ton_abi.len() {
        return Err(anyhow!("TON and ETH ABI are different")); // unreachable!
    }

    let abi_version = 2;
    let mut cursor = data.into();
    let mut tokens = Vec::with_capacity(eth_abi.len());

    for (eth_param_type, ton_param_type) in eth_abi.iter().zip(ton_abi.iter()) {
        let last = Some(ton_param_type) == ton_abi.last();

        let (token_value, new_cursor) =
            TokenValue::read_from(ton_param_type, cursor, last, abi_version)
                .map_err(|e| anyhow!(e))?;

        cursor = new_cursor;

        tokens.push(map_ton_eth(token_value, eth_param_type));
    }

    if cursor.remaining_references() != 0 || cursor.remaining_bits() != 0 {
        Err(anyhow!("incomplete event data deserialization"))
    } else {
        Ok(tokens)
    }
}

pub fn map_eth_abi(abi: &[EthParamType]) -> Result<Vec<TonParamType>, Error> {
    abi.iter().map(map_eth_abi_param).collect()
}

pub fn map_eth_abi_param(param: &EthParamType) -> Result<TonParamType, Error> {
    Ok(match param {
        EthParamType::Address => TonParamType::Bytes,
        EthParamType::Bytes => TonParamType::Bytes,
        EthParamType::Int(size) => TonParamType::Int(*size),
        EthParamType::Uint(size) => TonParamType::Uint(*size),
        EthParamType::Bool => TonParamType::Bool,
        EthParamType::String => TonParamType::Bytes,
        EthParamType::Array(param) => {
            TonParamType::Array(Box::new(map_eth_abi_param(param.as_ref())?))
        }
        EthParamType::FixedBytes(size) => TonParamType::FixedBytes(*size),
        EthParamType::FixedArray(param, size) => {
            TonParamType::FixedArray(Box::new(map_eth_abi_param(param.as_ref())?), *size)
        }
        EthParamType::Tuple(params) => TonParamType::Tuple(
            params
                .iter()
                .map(|item: &Box<EthParamType>| {
                    Ok(ton_abi::Param {
                        name: String::new(),
                        kind: map_eth_abi_param(item.as_ref())?,
                    })
                })
                .collect::<Result<Vec<ton_abi::Param>, Error>>()?,
        ),
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

// TODO: return result
pub fn map_ton_eth(ton: TonTokenValue, eth_param_type: &EthParamType) -> EthTokenValue {
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
        TonTokenValue::Bytes(a) => match eth_param_type {
            ParamType::Address if a.len() == 20 => {
                EthTokenValue::Address(ethereum_types::Address::from_slice(&a))
            }
            /*ParamType::Bytes*/ _ => EthTokenValue::Bytes(a),
        },
        TonTokenValue::Address(a) => EthTokenValue::String(a.to_string()),
        TonTokenValue::FixedBytes(a) => EthTokenValue::FixedBytes(a),
        TonTokenValue::Bool(a) => EthTokenValue::Bool(a),
        TonTokenValue::Cell(a) => EthTokenValue::Bytes(a.data().to_vec()),
        _ => todo!(),
    }
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

    use crate::engine::bridge::util::{
        eth_param_from_str, map_eth_ton, map_ton_eth, parse_eth_abi,
    };

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
  }
  "#;

    #[test]
    fn test_event_contract_abi() {
        let hash = parse_eth_abi(ABI).unwrap().0;
        let expected = H256::from_slice(&*Keccak256::digest(b"StateChange(uint256,address)"));
        assert_eq!(expected, hash);
    }
    //todo test abi parsing

    #[test]
    fn test_u256() {
        let expected = ParamType::Uint(256);
        let got = eth_param_from_str("uint256").unwrap();
        assert_eq!(expected, got);
    }
    #[test]
    fn test_i64() {
        let expected = ParamType::Int(64);
        let got = eth_param_from_str("Int64").unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn test_bytes() {
        let expected = ParamType::FixedBytes(32);
        let got = eth_param_from_str("bytes32").unwrap();
        assert_eq!(expected, got);
    }

    #[test]
    fn test_addr() {
        let expected = ParamType::Address;
        let got = eth_param_from_str("address").unwrap();
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
