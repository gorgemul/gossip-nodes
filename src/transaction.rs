use crate::kv::KV;
use anyhow::Result;
use serde_json::{Value, json};

pub struct Transaction<'a> {
    kv: KV<'a>,
}

impl<'a> Transaction<'a> {
    pub fn new(kv: KV<'a>) -> Self {
        Self { kv }
    }
    pub fn read(&self, key: u64) -> Value {
        match self.kv.read(&key.to_string()) {
            Ok(value) => value,
            Err(err) => {
                eprintln!("Transaction read error: {}", err);
                json!(null)
            }
        }
    }
    pub fn write(&self, key: u64, value: &Value) -> Result<()> {
        self.kv.write(&key.to_string(), value)?;
        Ok(())
    }
}
