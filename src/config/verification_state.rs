use std::path::Path;

use anyhow::Result;
use nekoton_utils::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddressVerificationState {
    #[serde(with = "serde_hex_array")]
    pub transaction_hash: [u8; 32],
    #[serde(with = "serde_hex_array")]
    pub address: [u8; 20],
}

impl AddressVerificationState {
    pub fn try_load<P>(path: P) -> Result<Option<Self>>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        Ok(if path.exists() {
            let file = std::fs::OpenOptions::new().read(true).open(path)?;
            let state = serde_json::from_reader(&file)?;
            Some(state)
        } else {
            None
        })
    }

    pub fn save<P>(&self, path: P) -> Result<()>
    where
        P: AsRef<Path>,
    {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;
        serde_json::to_writer_pretty(file, self)?;
        Ok(())
    }
}
