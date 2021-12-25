use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Default)]
pub struct Tristate(AtomicU8);

impl Tristate {
    pub fn new(data: Option<bool>) -> Self {
        let result = Self::default();
        result.store(data);
        result
    }

    pub fn store(&self, data: Option<bool>) {
        self.0.store(
            match data {
                Some(false) => 0,
                Some(true) => 1,
                None => 2,
            },
            Ordering::Release,
        );
    }

    pub fn store_if_empty(&self, data: bool) {
        self.0
            .compare_exchange(2, data as u8, Ordering::Release, Ordering::Relaxed)
            .ok();
    }

    pub fn load(&self) -> Option<bool> {
        match self.0.load(Ordering::Acquire) {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        }
    }
}
