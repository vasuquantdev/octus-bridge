use ton_abi::{Token, TokenValue};
use ton_types::Cell;

use super::errors::*;
use crate::prelude::*;

const ABI_VERSION: u8 = 2;

pub fn pack_tokens<T>(tokens: T) -> ContractResult<Cell>
where
    T: PackIntoCell,
{
    tokens.pack_into_cell()
}

pub trait PackIntoCell {
    fn pack_into_cell(self) -> ContractResult<Cell>;
}

impl<T> PackIntoCell for &T
where
    T: AsRef<[Token]>,
{
    fn pack_into_cell(self) -> ContractResult<Cell> {
        TokenValue::pack_values_into_chain(self.as_ref(), Vec::new(), ABI_VERSION)
            .and_then(|data| data.into_cell())
            .map_err(|_| ContractError::InvalidAbi)
    }
}

impl PackIntoCell for Vec<TokenValue> {
    fn pack_into_cell(self) -> ContractResult<Cell> {
        let tokens = self
            .into_iter()
            .map(|value| Token {
                name: String::new(),
                value,
            })
            .collect::<Vec<_>>();

        TokenValue::pack_values_into_chain(&tokens, Vec::new(), ABI_VERSION)
            .and_then(|data| data.into_cell())
            .map_err(|_| ContractError::InvalidAbi)
    }
}

pub fn make_address(addr: &str) -> ContractResult<MsgAddrStd> {
    match MsgAddressInt::from_str(addr) {
        Ok(MsgAddressInt::AddrStd(addr_std)) => Ok(addr_std),
        _ => Err(ContractError::InvalidAddress),
    }
}
