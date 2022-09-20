use std::convert::TryInto;
use std::future::Future;
use std::time::Duration;

use tryhard::backoff_strategies::{BackoffStrategy, ExponentialBackoff, FixedBackoff};
use tryhard::{NoOnRetry, RetryFutureConfig, RetryPolicy};

use solana_client::client_error::ClientError;
use solana_client::client_error::ClientErrorKind;
use solana_client::rpc_request::{RpcError, RpcResponseErrorData};
use solana_sdk::instruction::InstructionError;
use solana_sdk::transaction::TransactionError;

/// Retries future, logging unsuccessful retries with `message`
pub async fn retry<MakeFutureT, T, E, Fut, BackoffT, OnRetryT>(
    producer: MakeFutureT,
    config: RetryFutureConfig<BackoffT, OnRetryT>,
    network: NetworkType,
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
            "Retrying {:?} {} with {} attempt. Next delay: {:?}. Error: {:?}",
            network,
            message,
            attempt,
            next_delay,
            error
        );
        std::future::ready(())
    });
    tryhard::retry_fn(producer).with_config(config).await
}

#[inline]
pub fn generate_default_timeout_config(
    total_time: Duration,
) -> RetryFutureConfig<ExponentialBackoff, NoOnRetry> {
    let max_delay = Duration::from_secs(600);
    let times = crate::utils::calculate_times_from_max_delay(
        Duration::from_secs(1),
        2f64,
        max_delay,
        total_time,
    );
    tryhard::RetryFutureConfig::new(times)
        .exponential_backoff(Duration::from_secs(1))
        .max_delay(Duration::from_secs(600))
}

#[inline]
pub fn generate_fixed_timeout_config(
    sleep_time: Duration,
    total_time: Duration,
) -> RetryFutureConfig<FixedBackoff, NoOnRetry> {
    let times = (total_time.as_secs() / sleep_time.as_secs())
        .try_into()
        .expect("Overflow");
    tryhard::RetryFutureConfig::new(times).fixed_backoff(sleep_time)
}

#[derive(Clone)]
pub struct SolRpcBackoffStrategy {
    inner: ExponentialBackoff,
}

impl<'a> BackoffStrategy<'a, ClientError> for SolRpcBackoffStrategy {
    type Output = RetryPolicy;

    fn delay(&mut self, attempt: u32, error: &'a ClientError) -> Self::Output {
        match &error.kind {
            ClientErrorKind::RpcError(RpcError::RpcResponseError {
                data: RpcResponseErrorData::SendTransactionPreflightFailure(_),
                ..
            }) => RetryPolicy::Break,
            ClientErrorKind::TransactionError(TransactionError::InstructionError(
                _,
                InstructionError::Custom(_),
            )) => RetryPolicy::Break,
            _ => RetryPolicy::Delay(self.inner.delay(attempt, error)),
        }
    }
}

#[inline]
pub fn generate_sol_rpc_backoff_config(
    total_time: Duration,
) -> RetryFutureConfig<SolRpcBackoffStrategy, NoOnRetry> {
    let max_delay = Duration::from_secs(600);
    let times = calculate_times_from_max_delay(Duration::from_secs(1), 2f64, max_delay, total_time);
    tryhard::RetryFutureConfig::new(times)
        .custom_backoff(SolRpcBackoffStrategy {
            inner: ExponentialBackoff::new(Duration::from_secs(1)),
        })
        .max_delay(Duration::from_secs(600))
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

pub enum NetworkType {
    SOL,
    EVM(u32),
}

impl std::fmt::Debug for NetworkType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            NetworkType::SOL => write!(f, "SOL"),
            NetworkType::EVM(chain_id) => write!(f, "EVM-{chain_id}"),
        }
    }
}
