use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{Mutex, Notify, RwLock};

#[derive(Clone)]
pub struct Semaphore {
    guard: Arc<RwLock<SemaphoreGuard>>,
    counter: Arc<Mutex<usize>>,
    done: Arc<Notify>,
}

impl Semaphore {
    pub fn new(count: usize) -> Self {
        Self {
            guard: Arc::new(RwLock::new(SemaphoreGuard {
                target: count,
                allow_notify: false,
            })),
            counter: Arc::new(Mutex::new(count)),
            done: Arc::new(Notify::new()),
        }
    }

    pub fn new_empty() -> Self {
        Self::new(0)
    }

    pub async fn wait(self) {
        let target = {
            let mut guard = self.guard.write().await;
            guard.allow_notify = true;
            guard.target
        };
        if *self.counter.lock().await >= target {
            return;
        }
        self.done.notified().await
    }

    pub async fn wait_count(self, count: usize) {
        *self.guard.write().await = SemaphoreGuard {
            target: count,
            allow_notify: true,
        };
        if *self.counter.lock().await >= count {
            return;
        }
        self.done.notified().await
    }

    pub async fn notify(&self) {
        let mut counter = self.counter.lock().await;
        *counter += 1;

        let guard = self.guard.read().await;
        if *counter >= guard.target && guard.allow_notify {
            self.done.notify();
        }
    }
}

struct SemaphoreGuard {
    target: usize,
    allow_notify: bool,
}

#[async_trait]
pub trait TryNotify {
    async fn try_notify(self);
}

#[async_trait]
impl TryNotify for Option<Semaphore> {
    async fn try_notify(self) {
        if let Some(semaphore) = self {
            semaphore.notify().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::time;
    use tokio::time::Duration;

    fn spawn_tasks(semaphore: Semaphore) -> usize {
        let mut target = 0;

        for i in 0..10 {
            target += 1;
            tokio::spawn({
                let semaphore = semaphore.clone();
                async move {
                    println!("Task {} started", i);
                    time::delay_for(Duration::from_secs(i)).await;
                    println!("Task {} complete", i);
                    semaphore.notify().await;
                }
            });
        }

        target
    }

    #[tokio::test]
    async fn semaphore_with_unknown_target_incomplete() {
        let semaphore = Semaphore::new_empty();
        let target_count = spawn_tasks(semaphore.clone());

        time::delay_for(Duration::from_secs(7)).await;
        println!("Waiting...");
        semaphore.wait_count(target_count).await;
        println!("Done");
    }

    #[tokio::test]
    async fn semaphore_with_unknown_target_complete() {
        let semaphore = Semaphore::new_empty();
        let target_count = spawn_tasks(semaphore.clone());

        time::delay_for(Duration::from_secs(11)).await;
        println!("Waiting...");
        semaphore.wait_count(target_count).await;
        println!("Done");
    }
}
