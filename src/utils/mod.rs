use ton_types::UInt256;

pub use self::existing_contract::*;
use std::future::Future;
use std::time::Duration;
use tryhard::backoff_strategies::BackoffStrategy;
use tryhard::{RetryFutureConfig, RetryPolicy};

mod existing_contract;

pub fn contains_account(shard: &ton_block::ShardIdent, account: &UInt256) -> bool {
    let shard_prefix = shard.shard_prefix_with_tag();
    if shard_prefix == ton_block::SHARD_FULL {
        true
    } else {
        let len = shard.prefix_len();
        let account_prefix = account_prefix(account, len as usize) >> (64 - len);
        let shard_prefix = shard_prefix >> (64 - len);
        account_prefix == shard_prefix
    }
}

pub fn account_prefix(account: &UInt256, len: usize) -> u64 {
    debug_assert!(len <= 64);

    let account = account.as_slice();

    let mut value: u64 = 0;

    let bytes = len / 8;
    for (i, byte) in account.iter().enumerate().take(bytes) {
        value |= (*byte as u64) << (8 * (7 - i));
    }

    let remainder = len % 8;
    if remainder > 0 {
        let r = account[bytes] >> (8 - remainder);
        value |= (r as u64) << (8 * (7 - bytes) + 8 - remainder);
    }

    value
}

/// retries future, logging unsuccessful retries with `message`
pub async fn retry<MakeFutureT, T, E, Fut, BackoffT, OnRetryT>(
    producer: MakeFutureT,
    config: RetryFutureConfig<BackoffT, OnRetryT>,
    message: &'static str,
) -> Result<T, E>
where
    MakeFutureT: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Debug,
    for<'a> BackoffT: BackoffStrategy<'a, E>,
    for<'a> <BackoffT as BackoffStrategy<'a, E>>::Output: Into<RetryPolicy>,
{
    let config = config.on_retry(|attempt, next_delay, error: &E| {
        log::error!(
            "Retrying {} with {} attempt. Next delay: {:?}. Error: {:?}",
            message,
            attempt,
            next_delay,
            error
        );
        std::future::ready(())
    });
    let res = tryhard::retry_fn(producer).with_config(config).await;
    res
}

/// Calculates required number of steps, to get sum of retries ≈ `total_retry_time`.
#[inline]
pub fn calculate_times_from_max_delay(
    start_delay: Duration,
    fraction: f64,
    maximum_delay: Duration,
    total_retry_time: Duration,
) -> u32 {
    let start_delay = start_delay.as_secs_f64();
    let maximum_delay = maximum_delay.as_secs_f64();
    let total_retry_time = total_retry_time.as_secs_f64();
    //calculate number of steps to saturate. E.G. If maximum timeout is 600, then you'll have 9 steps, before reaching it.
    let saturation_steps =
        (f64::log10((maximum_delay - start_delay) / start_delay) / f64::log10(fraction)).floor();
    let time_to_saturate =
        start_delay * (1f64 - fraction.powf(saturation_steps)) / (1f64 - fraction);
    let remaining_time = total_retry_time - time_to_saturate;
    let steps = remaining_time / maximum_delay;
    (steps + saturation_steps).ceil() as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_account_prefix() {
        let mut account_id = [0u8; 32];
        for byte in account_id.iter_mut().take(8) {
            *byte = 0xff;
        }

        let account_id = UInt256::from(account_id);
        for i in 0..64 {
            let prefix = account_prefix(&account_id, i);
            assert_eq!(64 - prefix.trailing_zeros(), i as u32);
        }
    }

    #[test]
    fn test_contains_account() {
        let account = ton_types::UInt256::from_be_bytes(
            &hex::decode("459b6795bf4d4c3b930c83fe7625cfee99a762e1e114c749b62bfa751b781fa5")
                .unwrap(),
        );

        let mut shards =
            vec![ton_block::ShardIdent::with_tagged_prefix(0, ton_block::SHARD_FULL).unwrap()];
        for _ in 0..4 {
            let mut new_shards = vec![];
            for shard in &shards {
                let (left, right) = shard.split().unwrap();
                new_shards.push(left);
                new_shards.push(right);
            }

            shards = new_shards;
        }

        let mut target_shard = None;
        for shard in shards {
            if !contains_account(&shard, &account) {
                continue;
            }

            if target_shard.is_some() {
                panic!("Account can't be in two shards");
            }
            target_shard = Some(shard);
        }

        assert!(
            matches!(target_shard, Some(shard) if shard.shard_prefix_with_tag() == 0x4800000000000000)
        );
    }
}
