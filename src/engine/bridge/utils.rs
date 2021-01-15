use ethabi::{ParamType as EthParamType, Token as EthTokenValue};
use ton_abi::{ParamType as TonParamType, Token as TonToken, TokenValue as TonTokenValue};

use relay_ton::contracts::message_builder::{BigUint256, FunctionArg};
use relay_ton::contracts::{ContractError, ContractResult, EthEventConfiguration, SwapBackEvent};

use crate::prelude::*;

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

pub fn validate_ethereum_event_configuration(config: &EthEventConfiguration) -> Result<(), Error> {
    let EthEventConfiguration { common, .. } = config;
    serde_json::from_str::<serde_json::Value>(&common.event_abi)
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

pub fn parse_eth_event_data(
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
            TonTokenValue::read_from(ton_param_type, cursor, last, abi_version)
                .map_err(|e| anyhow!(e))?;

        cursor = new_cursor;

        tokens.push(map_ton_to_eth_with_abi(
            token_value,
            eth_param_type.clone(),
        )?);
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
                .map(|item| {
                    Ok(ton_abi::Param {
                        name: String::new(),
                        kind: map_eth_abi_param(item.as_ref())?,
                    })
                })
                .collect::<Result<Vec<ton_abi::Param>, Error>>()?,
        ),
    })
}

pub fn map_eth_to_ton(eth: EthTokenValue) -> TonTokenValue {
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
        EthTokenValue::Bool(a) => TonTokenValue::Bool(a),
        EthTokenValue::FixedArray(a) => {
            TonTokenValue::FixedArray(a.into_iter().map(map_eth_to_ton).collect())
        }
        EthTokenValue::Array(a) => {
            TonTokenValue::Array(a.into_iter().map(map_eth_to_ton).collect::<Vec<_>>())
        }
        EthTokenValue::Tuple(a) => TonTokenValue::Tuple(
            a.into_iter()
                .map(map_eth_to_ton)
                .map(|x| ton_abi::Token::new("", x))
                .collect::<Vec<_>>(),
        ),
    }
}

/// maps ton token to ethereum token according to abi in eth and ton
pub fn map_ton_to_eth_with_abi(
    ton: TonTokenValue,
    eth_param_type: EthParamType,
) -> Result<EthTokenValue, Error> {
    Ok(match (ton, eth_param_type) {
        (TonTokenValue::FixedBytes(bytes), EthParamType::FixedBytes(size))
            if bytes.len() == size =>
        {
            EthTokenValue::FixedBytes(bytes)
        }
        (TonTokenValue::Bytes(a), EthParamType::Bytes) => EthTokenValue::Bytes(a),
        (TonTokenValue::Uint(a), EthParamType::Uint(_)) => {
            let bytes = a.number.to_bytes_le();
            EthTokenValue::Uint(ethabi::Uint::from_little_endian(&bytes))
        }
        (TonTokenValue::Int(a), EthParamType::Int(_)) => {
            let mut bytes = a.number.to_signed_bytes_le();
            let sign = bytes
                .last()
                .map(|first| (first >> 7) * 255)
                .unwrap_or_default();
            bytes.resize(32, sign);

            EthTokenValue::Int(ethabi::Int::from_little_endian(&bytes))
        }
        (TonTokenValue::Bytes(a), EthParamType::Address) if a.len() == 20 => {
            EthTokenValue::Address(ethereum_types::Address::from_slice(&a))
        }
        (TonTokenValue::Bytes(a), EthParamType::String) => {
            EthTokenValue::String(String::from_utf8(a)?)
        }
        (TonTokenValue::Bool(a), EthParamType::Bool) => EthTokenValue::Bool(a),
        (TonTokenValue::FixedArray(tokens), EthParamType::FixedArray(eth_param_type, size))
            if tokens.len() == size =>
        {
            EthTokenValue::FixedArray(
                tokens
                    .into_iter()
                    .take(size)
                    .map(|ton| map_ton_to_eth_with_abi(ton, *eth_param_type.clone()))
                    .collect::<Result<_, _>>()?,
            )
        }
        (TonTokenValue::Array(tokens), EthParamType::Array(eth_param_type)) => {
            EthTokenValue::Array(
                tokens
                    .into_iter()
                    .map(|ton| map_ton_to_eth_with_abi(ton, *eth_param_type.clone()))
                    .collect::<Result<_, _>>()?,
            )
        }
        (TonTokenValue::Tuple(tokens), EthParamType::Tuple(params))
            if tokens.len() == params.len() =>
        {
            EthTokenValue::Tuple(
                tokens
                    .into_iter()
                    .zip(params.into_iter())
                    .map(|(ton, eth_param_type)| {
                        map_ton_to_eth_with_abi(ton.value, *eth_param_type)
                    })
                    .collect::<Result<_, _>>()?,
            )
        }
        _ => return Err(anyhow!("unsupported type")),
    })
}

/// naively maps ton tokens ti ethereum tokens
fn map_ton_to_eth(token: TonTokenValue) -> Result<EthTokenValue, Error> {
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
                .map(map_ton_to_eth)
                .collect::<Result<_, _>>()?,
        ),
        TonTokenValue::Array(tokens) => EthTokenValue::Array(
            tokens
                .into_iter()
                .map(map_ton_to_eth)
                .collect::<Result<_, _>>()?,
        ),
        TonTokenValue::Tuple(tokens) => EthTokenValue::Tuple(
            tokens
                .into_iter()
                .map(|ton| map_ton_to_eth(ton.value))
                .collect::<Result<_, _>>()?,
        ),
        any => return Err(anyhow!("unsupported type: {:?}", any)),
    })
}

pub fn prepare_ton_event_payload(event: &SwapBackEvent) -> Result<Vec<u8>, Error> {
    // struct TONEvent {
    //     uint eventTransaction;
    //     uint eventIndex;
    //     bytes eventData;
    //     uint eventBlockNumber;
    //     uint eventBlock;
    //     bytes tonEventConfiguration;
    //     uint requiredConfirmations;
    //     uint requiredRejects;
    // }

    let event_index = BigUint256(event.event_index.into());
    let event_data = ton_tokens_to_ethereum_bytes(event.tokens.clone());
    let event_block_number = BigUint256(event.event_transaction_lt.into());

    let tuple = EthTokenValue::Tuple(vec![
        map_ton_to_eth(event.event_transaction.clone().token_value())?,
        map_ton_to_eth(event_index.token_value())?,
        map_ton_to_eth(event_data.token_value())?,
        map_ton_to_eth(event_block_number.token_value())?,
        map_ton_to_eth(UInt256::default().token_value())?, // eventBlock
        map_ton_to_eth(Vec::<u8>::default().token_value())?, // tonEventConfiguration
        map_ton_to_eth(UInt256::default().token_value())?, // requiredConfirmations
        map_ton_to_eth(UInt256::default().token_value())?, //requiredRejects
    ]);

    Ok(ethabi::encode(&[tuple]).to_vec())
}

///maps `Vec<TonTokenValue>` to bytes, which could be signed
pub fn ton_tokens_to_ethereum_bytes(tokens: Vec<ton_abi::Token>) -> Vec<u8> {
    let tokens: Vec<_> = tokens
        .into_iter()
        .map(|token| token.value)
        .map(map_ton_to_eth)
        .filter_map(|x| match x {
            Ok(a) => Some(a),
            Err(e) => {
                log::error!("Failed mapping ton token to eth token: {}", e);
                None
            }
        })
        .collect();

    ethabi::encode(&tokens).to_vec()
}

pub fn pack_token_values(token_values: Vec<TonTokenValue>) -> ContractResult<Cell> {
    let tokens = token_values
        .into_iter()
        .map(|value| TonToken {
            name: String::new(),
            value,
        })
        .collect::<Vec<_>>();

    TonTokenValue::pack_values_into_chain(&tokens, Vec::new(), TON_ABI_VERSION)
        .and_then(|data| data.into_cell())
        .map_err(|_| ContractError::InvalidAbi)
}

pub fn pack_event_data_into_cell(event_id: u32, tokens: &[TonToken]) -> ContractResult<Cell> {
    let event_id_prefix = SliceData::from(&event_id.to_be_bytes()[..]);

    TonTokenValue::pack_values_into_chain(
        tokens,
        vec![BuilderData::from_slice(&event_id_prefix)],
        TON_ABI_VERSION,
    )
    .and_then(|data| data.into_cell())
    .map_err(|_| ContractError::InvalidAbi)
}

const TON_ABI_VERSION: u8 = 2;

#[cfg(test)]
mod test {
    use ethabi::ParamType;
    use ethabi::Token as EthTokenValue;
    use num_bigint::{BigInt, BigUint};
    use sha3::Digest;
    use sha3::Keccak256;
    use ton_abi::TokenValue as TonTokenValue;

    use relay_eth::ws::H256;
    use relay_ton::contracts::utils::pack_token_values;
    use relay_ton::prelude::serialize_toc;

    use crate::engine::bridge::utils::{
        eth_param_from_str, map_eth_to_ton, map_ton_to_eth_with_abi, parse_eth_abi,
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

    #[test]
    fn test_event_contract_abi() {
        let hash = parse_eth_abi(ABI).unwrap().0;
        let expected = H256::from_slice(&*Keccak256::digest(b"StateChange(uint256,address)"));
        assert_eq!(expected, hash);
    }

    #[test]
    fn test_event_contract_abi2() {
        let hash = parse_eth_abi(ABI2).unwrap().0;
        let expected = H256::from_slice(&*Keccak256::digest(b"EthereumStateChange(uint256)"));
        assert_eq!(expected, hash);
    }

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
        assert_eq!(map_eth_to_ton(eth), ton_expected);
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
        assert_eq!(map_eth_to_ton(eth), ton_expected);
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
        assert_eq!(map_eth_to_ton(eth), ton_expected);
        println!(
            "{}",
            base64::encode(serialize_toc(&pack_token_values(vec![ton_expected]).unwrap()).unwrap())
        );
    }

    #[test]
    fn test_conversion_tuple() {
        use ethabi::Uint as EUnt;
        use ton_abi::Uint as TUInt;

        let number = make_int256_le(1234567);
        let ton_token_uint = ton_abi::Token {
            name: "".to_string(),
            value: TonTokenValue::Uint(TUInt {
                number: BigUint::from_bytes_le(&number),
                size: 256,
            }),
        };
        let ton_token_bytes = ton_abi::Token {
            name: "".to_string(),
            value: TonTokenValue::Bytes("hello from rust".to_string().into()),
        };
        let eth_token_uint = ethabi::Token::Uint(EUnt::from_little_endian(&number));
        let eth_token_bytes = ethabi::Token::Bytes("hello from rust".to_string().into());
        let eth = EthTokenValue::Tuple(vec![eth_token_uint, eth_token_bytes]);
        let ton_expected = TonTokenValue::Tuple(vec![ton_token_uint, ton_token_bytes]);
        assert_eq!(map_eth_to_ton(eth), ton_expected);
        println!(
            "{}",
            base64::encode(serialize_toc(&pack_token_values(vec![ton_expected]).unwrap()).unwrap())
        );
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
        assert_eq!(
            map_ton_to_eth_with_abi(ton, ethabi::ParamType::Int(256)).unwrap(),
            eth_expected
        );
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
        let got = map_ton_to_eth_with_abi(ton_expected, ethabi::ParamType::Int(256));
        assert_eq!(got.unwrap(), eth);
    }

    #[test]
    fn ton_test_conversion_uint() {
        use ethabi::Uint as EUint;
        use ton_abi::Uint as TUint;
        let eth = EthTokenValue::Uint(EUint::from(1234567));
        let ton_expected = TonTokenValue::Uint(TUint::new(1234567, 256));
        assert_eq!(
            map_ton_to_eth_with_abi(ton_expected, ethabi::ParamType::Uint(256)).unwrap(),
            eth
        );
    }
}
