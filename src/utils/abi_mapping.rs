use anyhow::Result;
use ethabi::{ParamType as EthParamType, Token as EthTokenValue};
use num_bigint::{BigInt, BigUint};
use serde::Deserialize;
use ton_abi::{ParamType as TonParamType, TokenValue as TonTokenValue};

pub fn decode_ton_event_abi(abi: &str) -> Result<Vec<ton_abi::Param>> {
    let params = serde_json::from_str::<Vec<ton_abi::Param>>(abi)?;
    Ok(params)
}

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

pub fn map_eth_abi_to_ton(abi: &[EthParamType]) -> Result<Vec<TonParamType>> {
    abi.iter().map(map_eth_abi_param_to_ton).collect()
}

fn map_eth_abi_param_to_ton(param: &EthParamType) -> Result<TonParamType> {
    Ok(match param {
        EthParamType::Address => TonParamType::Bytes,
        EthParamType::Bytes => TonParamType::Bytes,
        EthParamType::Int(size) => TonParamType::Int(*size),
        EthParamType::Uint(size) => TonParamType::Uint(*size),
        EthParamType::Bool => TonParamType::Bool,
        EthParamType::String => TonParamType::Bytes,
        EthParamType::Array(param) => {
            TonParamType::Array(Box::new(map_eth_abi_param_to_ton(param.as_ref())?))
        }
        EthParamType::FixedBytes(size) => TonParamType::FixedBytes(*size),
        EthParamType::FixedArray(param, size) => {
            TonParamType::FixedArray(Box::new(map_eth_abi_param_to_ton(param.as_ref())?), *size)
        }
        EthParamType::Tuple(params) => TonParamType::Tuple(
            params
                .iter()
                .map(|item| {
                    Ok(ton_abi::Param {
                        name: String::new(),
                        kind: map_eth_abi_param_to_ton(item)?,
                    })
                })
                .collect::<Result<Vec<ton_abi::Param>>>()?,
        ),
    })
}

/// Maps `Vec<TonTokenValue>` to bytes, which could be signed
pub fn map_ton_tokens_to_eth_bytes(tokens: Vec<ton_abi::Token>) -> Result<Vec<u8>> {
    let tokens = tokens
        .into_iter()
        .map(|token| token.value)
        .map(map_ton_token_to_eth)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ethabi::encode(&tokens))
}

pub fn map_eth_to_ton_with_abi(
    eth: EthTokenValue,
    eth_param_abi: &EthParamType,
) -> Result<TonTokenValue> {
    Ok(match (eth, eth_param_abi) {
        (EthTokenValue::FixedBytes(x), _) => TonTokenValue::FixedBytes(x.to_vec()),
        (EthTokenValue::Bytes(x), _) => TonTokenValue::Bytes(x.to_vec()),
        (EthTokenValue::Uint(x), &EthParamType::Uint(size)) => {
            let mut bytes = [0u8; 256 / 8];
            x.to_big_endian(&mut bytes);
            let number = BigUint::from_bytes_be(&bytes);
            TonTokenValue::Uint(ton_abi::Uint { number, size })
        }
        (EthTokenValue::Int(x), &EthParamType::Int(size)) => {
            let mut bytes = [0u8; 256 / 8];
            x.to_big_endian(&mut bytes);
            let number = BigInt::from_signed_bytes_be(&bytes);
            TonTokenValue::Int(ton_abi::Int { number, size })
        }
        (EthTokenValue::Address(ad), _) => TonTokenValue::Bytes(ad.0.to_vec()),
        (EthTokenValue::String(a), _) => TonTokenValue::Bytes(Vec::from(a)),
        (EthTokenValue::Bool(a), _) => TonTokenValue::Bool(a),
        (EthTokenValue::FixedArray(a), EthParamType::FixedArray(abi, _)) => {
            let param_type = match *abi.clone() {
                EthParamType::Array(arr) => {
                    let mut mapped = map_eth_abi_to_ton(&[*arr])?;
                    anyhow::ensure!(!mapped.is_empty(), "No types");
                    mapped.remove(0)
                }
                _ => anyhow::bail!("Bad abi"),
            }
            .clone();
            TonTokenValue::FixedArray(
                param_type,
                a.into_iter()
                    .map(|value| map_eth_to_ton_with_abi(value, abi))
                    .collect::<Result<Vec<_>, _>>()?,
            )
        }
        (EthTokenValue::Array(a), EthParamType::Array(abi)) => {
            let param_type = match *abi.clone() {
                EthParamType::Array(arr) => {
                    let mut mapped = map_eth_abi_to_ton(&[*arr])?;
                    anyhow::ensure!(!mapped.is_empty(), "No types");
                    mapped.remove(0)
                }
                _ => anyhow::bail!("Bad abi"),
            }
            .clone();
            TonTokenValue::Array(
                param_type,
                a.into_iter()
                    .map(|value| map_eth_to_ton_with_abi(value, abi))
                    .collect::<Result<Vec<_>, _>>()?,
            )
        }
        (EthTokenValue::Tuple(a), EthParamType::Tuple(abi)) => TonTokenValue::Tuple(
            a.into_iter()
                .zip(abi.iter())
                .map(|(value, abi)| {
                    map_eth_to_ton_with_abi(value, abi).map(|x| ton_abi::Token::new("", x))
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        ty => return Err(AbiMappingError::UnsupportedEthType(ty.0).into()),
    })
}

fn map_ton_token_to_eth(token: TonTokenValue) -> Result<EthTokenValue, AbiMappingError> {
    Ok(match token {
        TonTokenValue::FixedBytes(bytes) => EthTokenValue::FixedBytes(bytes),
        TonTokenValue::Bytes(bytes) => EthTokenValue::Bytes(bytes),
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
            //fixme check it
            EthTokenValue::Int(ethabi::Int::from_little_endian(&bytes))
        }
        TonTokenValue::Bool(a) => EthTokenValue::Bool(a),
        TonTokenValue::FixedArray(_, tokens) => EthTokenValue::FixedArray(
            tokens
                .into_iter()
                .map(map_ton_token_to_eth)
                .collect::<Result<_, _>>()?,
        ),
        TonTokenValue::Array(_, tokens) => EthTokenValue::Array(
            tokens
                .into_iter()
                .map(map_ton_token_to_eth)
                .collect::<Result<_, _>>()?,
        ),
        TonTokenValue::Tuple(tokens) => EthTokenValue::Tuple(
            tokens
                .into_iter()
                .map(|ton| map_ton_token_to_eth(ton.value))
                .collect::<Result<_, _>>()?,
        ),
        any => return Err(AbiMappingError::UnsupportedTonType(any)),
    })
}

#[derive(thiserror::Error, Debug)]
enum AbiMappingError {
    #[error("Unsupported type: {:?}", .0)]
    UnsupportedTonType(ton_abi::TokenValue),
    #[error("Unsupported type: {:?}", .0)]
    UnsupportedEthType(ethabi::Token),
}

#[cfg(test)]
mod test {
    use super::map_ton_token_to_eth;
    use super::EthParamType;
    use super::EthTokenValue;
    use super::TonParamType;
    use super::TonTokenValue;
    use crate::utils::map_eth_to_ton_with_abi;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_eth_ton() {
        let types = [
            EthParamType::Address,
            EthParamType::String,
            EthParamType::Int(128),
            EthParamType::Tuple(vec![EthParamType::Bool, EthParamType::Bytes]),
        ];
        let got = super::map_eth_abi_to_ton(&types).unwrap();
        let expected = vec![
            TonParamType::Bytes,
            TonParamType::Bytes,
            TonParamType::Int(128),
            TonParamType::Tuple(vec![
                ton_abi::Param {
                    name: "".to_string(),
                    kind: TonParamType::Bool,
                },
                ton_abi::Param {
                    name: "".to_string(),
                    kind: TonParamType::Bytes,
                },
            ]),
        ];
        assert_eq!(got, expected);
    }

    fn test_int(size: u16, number: i128) {
        let ton_token = TonTokenValue::Int(ton_abi::Int::new(number, size as usize));
        let eth_token = map_ton_token_to_eth(ton_token).unwrap();
        match eth_token {
            EthTokenValue::Int(a) => {
                assert_eq!(a.to_string(), number.to_string());
            }
            _ => unreachable!(),
        }
    }

    fn test_uint(size: u16, number: u128) {
        let ton_token = TonTokenValue::Uint(ton_abi::Uint::new(number, size as usize));
        let eth_token = map_ton_token_to_eth(ton_token).unwrap();
        match eth_token {
            EthTokenValue::Uint(a) => {
                assert_eq!(a.as_u128(), number);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_ton_eth_uint() {
        let mut i = 1;
        loop {
            test_uint(128, i as u128);
            if i >= u128::MAX / 2 {
                break;
            }
            i *= 2;
        }
    }

    #[test]
    fn test_ton_eth_int() {
        let mut i = 1;
        loop {
            test_int(128, i);
            if i >= i128::MAX / 2 {
                break;
            }
            i *= 2;
        }
    }

    #[test]
    fn test_conversion_tuple() {
        use ethabi::Uint as EUnt;
        use ton_abi::Uint as TUInt;

        let number = 1234567;
        let ton_token_uint = ton_abi::Token {
            name: "".to_string(),
            value: TonTokenValue::Uint(TUInt::new(number, 256)),
        };
        let ton_token_bytes = ton_abi::Token {
            name: "".to_string(),
            value: TonTokenValue::Bytes("hello from rust".to_string().into()),
        };
        let eth_token_uint = ethabi::Token::Uint(EUnt::from(number));
        let eth_token_bytes = ethabi::Token::Bytes("hello from rust".to_string().into());
        let eth = EthTokenValue::Tuple(vec![eth_token_uint, eth_token_bytes]);
        let ton_expected = TonTokenValue::Tuple(vec![ton_token_uint, ton_token_bytes]);
        assert_eq!(
            map_eth_to_ton_with_abi(
                eth,
                &ethabi::ParamType::Tuple(vec![
                    ethabi::ParamType::Uint(256),
                    ethabi::ParamType::Bytes
                ]),
            )
            .unwrap(),
            ton_expected
        );
    }
}
