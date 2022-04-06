use borsh::BorshDeserialize;
use solana_program::hash::Hash;

use solana_program::message::Message;
use solana_sdk::instruction::CompiledInstruction;
use solana_sdk::pubkey::Pubkey;
use token_proxy::TokenProxyInstruction;

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

pub fn create_confirm_message(
    payer: &Pubkey,
    relay_pubkey: &Pubkey,
    payload_id: Hash,
    round_number: u32,
) -> Message {
    let ix = token_proxy::confirm_withdrawal_request(relay_pubkey, payload_id, round_number);
    Message::new(&[ix], Some(payer))
}
