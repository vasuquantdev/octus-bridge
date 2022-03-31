use borsh::BorshDeserialize;

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
