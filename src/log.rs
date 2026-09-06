use crate::kv::KV;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
struct LogEntry {
    value: Value,
    is_committed: bool,
}

pub struct Log<'a> {
    kv: KV<'a>,
}

fn get_offset_key(key: &str) -> String {
    format!("offset-{key}")
}

fn get_entry_key(key: &str, offset: u64) -> String {
    format!("entries-{key}-{offset}")
}

impl<'a> Log<'a> {
    pub fn new(kv: KV<'a>) -> Self {
        Self { kv }
    }
    pub fn append(&self, key: &str, value: &Value) -> Result<u64> {
        loop {
            let offset_key = get_offset_key(key);
            let offset = self
                .kv
                .read(&offset_key)
                .unwrap_or(json!(0))
                .as_u64()
                .unwrap();
            let claimed_offset = match self.kv.compare_and_swap(&offset_key, offset, offset + 1) {
                Ok(_) => offset,
                Err(err) => {
                    eprintln!("Log append retry: {}", err);
                    continue;
                }
            };
            self.kv
                .write(
                    &get_entry_key(key, claimed_offset),
                    LogEntry {
                        is_committed: false,
                        value: value.to_owned(),
                    },
                )
                .unwrap_or_else(|_| {
                    panic!("Fatal error: offset={offset_key} is claimed but fail to write content")
                });
            return Ok(claimed_offset);
        }
    }
    pub fn read(
        &self,
        key_to_offset: &HashMap<&str, u64>,
    ) -> Result<HashMap<String, Vec<(u64, Value)>>> {
        let mut result: HashMap<String, Vec<(u64, Value)>> = HashMap::new();
        for (&key, &offset) in key_to_offset {
            let mut i = offset;
            let mut entries: Vec<(u64, Value)> = Vec::new();
            loop {
                let Ok(entry) = self.kv.read(&get_entry_key(key, i)) else {
                    break;
                };
                let entry: LogEntry = serde_json::from_value(entry)?;
                entries.push((i, entry.value));
                i += 1;
            }
            if !entries.is_empty() {
                result.insert(key.to_owned(), entries);
            }
        }
        Ok(result)
    }
    pub fn commit(&self, key_to_offset: &HashMap<&str, u64>) -> Result<()> {
        for (&key, &offset) in key_to_offset {
            for i in 0..offset + 1 {
                let entry_key = get_entry_key(key, i);
                let Ok(entry) = self.kv.read(&entry_key) else {
                    break;
                };
                let mut entry: LogEntry = serde_json::from_value(entry)?;
                if !entry.is_committed {
                    entry.is_committed = true;
                    self.kv.write(&entry_key, entry).unwrap_or_else(|_| {
                        panic!(
                            "Fatal error: entry_key={entry_key} is read but fail to write content"
                        )
                    });
                }
            }
        }
        Ok(())
    }
    pub fn read_committed(&self, keys: &[&str]) -> Result<HashMap<String, u64>> {
        let mut result: HashMap<String, u64> = HashMap::new();
        for &key in keys {
            let mut last_committed_offset: Option<u64> = None;
            let mut offset = 0u64;
            loop {
                let entry_key = get_entry_key(key, offset);
                let Ok(entry) = self.kv.read(&entry_key) else {
                    break;
                };
                let entry: LogEntry = serde_json::from_value(entry)?;
                if !entry.is_committed {
                    break;
                }
                last_committed_offset = Some(offset);
                offset += 1;
            }
            if let Some(offset) = last_committed_offset {
                result.insert(key.to_owned(), offset);
            }
        }
        Ok(result)
    }
}
