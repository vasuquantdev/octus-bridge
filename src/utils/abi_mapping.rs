use anyhow::Result;
use ethabi::{ParamType as EthParamType, Token as EthTokenValue};
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
        TonTokenValue::FixedArray(tokens) => EthTokenValue::FixedArray(
            tokens
                .into_iter()
                .map(map_ton_token_to_eth)
                .collect::<Result<_, _>>()?,
        ),
        TonTokenValue::Array(tokens) => EthTokenValue::Array(
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
        any => return Err(AbiMappingError::UnsupportedType(any)),
    })
}

#[derive(thiserror::Error, Debug)]
enum AbiMappingError {
    #[error("Unsupported type: {:?}", .0)]
    UnsupportedType(ton_abi::TokenValue),
}
