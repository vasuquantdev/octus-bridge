use std::convert::TryInto;
use std::future::Future;
use std::time::Duration;

use tryhard::backoff_strategies::{BackoffStrategy, ExponentialBackoff, FixedBackoff};
use tryhard::{NoOnRetry, RetryFutureConfig, RetryPolicy};

/// Retries future, logging unsuccessful retries with `message`
pub async fn retry<MakeFutureT, T, E, Fut, BackoffT, OnRetryT>(
    producer: MakeFutureT,
    config: RetryFutureConfig<BackoffT, OnRetryT>,
    chain_id: u32,
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
            "Retrying EVM-{chain_id} {} with {} attempt. Next delay: {:?}. Error: {:?}",
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
