use borsh::BorshDeserialize;
use token_proxy::{TokenProxyInstruction, Vote};
use ton_types::UInt256;

use solana_sdk::instruction::CompiledInstruction;
use solana_sdk::message::Message;
use solana_sdk::pubkey::Pubkey;

pub fn decode_token_proxy_instruction(
    program_pubkey: Pubkey,
    instruction: &CompiledInstruction,
    account_keys: &[Pubkey],
) -> Option<TokenProxyInstruction> {
    let id: usize = instruction.program_id_index as usize;

    if account_keys[id] == program_pubkey {
        TokenProxyInstruction::try_from_slice(&instruction.data).ok()
    } else {
        None
    }
}

pub fn create_vote_message(
    relay_pubkey: &Pubkey,
    round_number: u32,
    event_configuration: &UInt256,
    event_transaction_lt: u64,
    vote: Vote,
) -> Message {
    let ix = token_proxy::vote_for_withdrawal_request(
        relay_pubkey,
        round_number,
        event_configuration,
        event_transaction_lt,
        vote,
    );
    Message::new(&[ix], Some(relay_pubkey))
}
