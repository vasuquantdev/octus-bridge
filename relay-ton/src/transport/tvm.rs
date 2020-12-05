use ton_block::{
    AccountStuff, CommonMsgInfo, CurrencyCollection, Deserializable, Message, OutAction,
    OutActions, Serializable,
};
use ton_types::SliceData;
use ton_vm::executor::gas::gas_state::Gas;
use ton_vm::stack::integer::IntegerData;
use ton_vm::stack::{savelist::SaveList, Stack, StackItem};

use crate::prelude::*;
use crate::transport::errors::*;

pub fn call(
    utime: u32,
    lt: u64,
    mut account: AccountStuff,
    stack: Stack,
) -> TransportResult<(ton_vm::executor::Engine, AccountStuff)> {
    let mut state = match &mut account.storage.state {
        ton_block::AccountState::AccountActive(state) => Ok(state),
        _ => Err(TransportError::ExecutionError {
            reason: "Account is not active".to_string(),
        }),
    }?;

    let mut ctrls = SaveList::new();
    ctrls
        .put(
            4,
            &mut StackItem::Cell(state.data.clone().unwrap_or_default()),
        )
        .map_err(|err| TransportError::ExecutionError {
            reason: format!("can not put data to registers: {}", err),
        })?;

    let sci = build_contract_info(&account.addr, &account.storage.balance, utime, lt, lt);
    ctrls
        .put(7, &mut sci.into_temp_data())
        .map_err(|err| TransportError::ExecutionError {
            reason: format!("can not put SCI to registers: {}", err),
        })?;

    let gas_limit = 1_000_000_000;
    let gas = Gas::new(gas_limit, 0, gas_limit, 10);

    let code = state.code.clone().ok_or(TransportError::ExecutionError {
        reason: "Account has no code".to_string(),
    })?;
    let mut engine = ton_vm::executor::Engine::new().setup(
        SliceData::from(code),
        Some(ctrls),
        Some(stack),
        Some(gas),
    );

    let result = engine.execute();

    match result {
        Err(err) => {
            let exception = ton_vm::error::tvm_exception(err).map_err(|err| {
                TransportError::ExecutionError {
                    reason: err.to_string(),
                }
            })?;
            let code = if let Some(code) = exception.custom_code() {
                code
            } else {
                !(exception
                    .exception_code()
                    .unwrap_or(ton_types::ExceptionCode::UnknownError) as i32)
            };

            Err(TransportError::ExecutionError {
                reason: format!(
                    "TVM Execution failed. {}, {}, {}",
                    exception.to_string(),
                    code,
                    &account.addr,
                ),
            })
        }
        Ok(_) => {
            match engine.get_committed_state().get_root() {
                StackItem::Cell(cell) => state.data = Some(cell),
                _ => {
                    return Err(TransportError::ExecutionError {
                        reason: "invalid commited state".to_string(),
                    })
                }
            };
            Ok((engine, account))
        }
    }
}

pub fn call_msg(
    utime: u32,
    lt: u64,
    account: AccountStuff,
    msg: &Message,
) -> TransportResult<(Vec<Message>, AccountStuff)> {
    let msg_cell = msg
        .write_to_new_cell()
        .map_err(|err| TransportError::FailedToSendMessage {
            reason: format!("can not serialize message: {}", err),
        })?;

    let mut stack = Stack::new();
    let balance = account.storage.balance.grams.value();
    let function_selector = match msg.header() {
        CommonMsgInfo::IntMsgInfo(_) => ton_vm::int!(0),
        CommonMsgInfo::ExtInMsgInfo(_) => ton_vm::int!(-1),
        CommonMsgInfo::ExtOutMsgInfo(_) => {
            return Err(TransportError::ExecutionError {
                reason: "invalid message type".to_owned(),
            })
        }
    };
    stack
        .push(ton_vm::int!(balance)) // token balance of contract
        .push(ton_vm::int!(0)) // token balance of msg
        .push(StackItem::Cell(msg_cell.into())) // message
        .push(StackItem::Slice(msg.body().unwrap_or_default())) // message body
        .push(function_selector); // function selector

    let (engine, account) = call(utime, lt, account, stack)?;

    // process out actions to get out messages
    let actions_cell = engine
        .get_actions()
        .as_cell()
        .map_err(|err| TransportError::ExecutionError {
            reason: format!("can not get actions: {}", err),
        })?
        .clone();
    let mut actions = OutActions::construct_from_cell(actions_cell).map_err(|err| {
        TransportError::ExecutionError {
            reason: format!("can not parse actions: {}", err),
        }
    })?;

    let mut msgs = vec![];
    for (_, action) in actions.iter_mut().enumerate() {
        if let OutAction::SendMsg { out_msg, .. } = std::mem::replace(action, OutAction::None) {
            msgs.push(out_msg);
        }
    }

    msgs.reverse();
    Ok((msgs, account))
}

fn build_contract_info(
    address: &MsgAddressInt,
    balance: &CurrencyCollection,
    block_unixtime: u32,
    block_lt: u64,
    tr_lt: u64,
) -> ton_vm::SmartContractInfo {
    let mut info =
        ton_vm::SmartContractInfo::with_myself(address.serialize().unwrap_or_default().into());
    *info.block_lt_mut() = block_lt;
    *info.trans_lt_mut() = tr_lt;
    *info.unix_time_mut() = block_unixtime;
    *info.balance_remaining_grams_mut() = balance.grams.0;
    *info.balance_remaining_other_mut() = balance.other_as_hashmap();

    info
}
