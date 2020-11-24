use ton_abi::{Token, TokenValue};
use ton_types::Cell;

use super::errors::*;
use crate::prelude::*;

pub fn pack_tokens(tokens: &[Token]) -> ContractResult<Cell> {
    TokenValue::pack_values_into_chain(tokens, Vec::new(), 2)
        .and_then(|data| data.into_cell())
        .map_err(|_| ContractError::InvalidAbi)
}

pub fn make_address(addr: &str) -> ContractResult<MsgAddrStd> {
    match MsgAddressInt::from_str(addr) {
        Ok(MsgAddressInt::AddrStd(addr_std)) => Ok(addr_std),
        _ => Err(ContractError::InvalidAddress),
    }
}
