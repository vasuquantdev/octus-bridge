use std::sync::atomic::{AtomicU32, Ordering};

use parking_lot::Mutex;
use tiny_adnl::utils::*;
use tokio::sync::oneshot;
use ton_types::UInt256;

use super::shard_utils::*;

pub struct PendingMessagesCache {
    min_expire_at: AtomicU32,
    entries: Mutex<FxHashMap<PendingMessageId, PendingMessage>>,
}

impl PendingMessagesCache {
    pub fn update(&self, current_utime: u32) {
        let current_min_expire_at: u32 = self.min_expire_at.load(Ordering::Acquire);
        if current_utime < current_min_expire_at {
            return;
        }

        let mut min_expire_at: u32 = 0;

        // let entries = self.entries.lock();
        // entries.retain(|(account, item)| {
        //     if !contains_account(account) {}
        //
        //     true
        // });

        todo!()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
struct PendingMessageId {
    address: UInt256,
    message_hash: UInt256,
}

struct PendingMessage {
    tx: Option<oneshot::Sender<MessageStatus>>,
    expire_at: u32,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum MessageStatus {
    Delivered,
    Expired,
}
