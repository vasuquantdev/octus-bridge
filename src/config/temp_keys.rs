use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::Result;

/// ADNL keys wrapper
pub struct TempKeys(ton_indexer::NodeKeys);

impl TempKeys {
    /// Load from disk.
    /// NOTE: generates and saves new if it doesn't exist
    pub fn load<P>(path: P, force_regenerate: bool) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;

        let temp_keys = if force_regenerate {
            ton_indexer::NodeKeys::generate()
        } else {
            match serde_json::from_reader(&file) {
                Ok(keys) => keys,
                Err(_) => {
                    log::error!("Failed to read temp keys. Generating new");
                    ton_indexer::NodeKeys::generate()
                }
            }
        };

        let result = Self(temp_keys);
        result.save(file)?;

        Ok(result)
    }

    fn save<W>(&self, mut file: W) -> Result<()>
    where
        W: Write + Seek,
    {
        file.seek(SeekFrom::Start(0))?;
        serde_json::to_writer_pretty(file, &self.0)?;
        Ok(())
    }
}

impl From<TempKeys> for ton_indexer::NodeKeys {
    fn from(keys: TempKeys) -> Self {
        keys.0
    }
}
