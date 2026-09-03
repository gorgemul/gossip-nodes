use crate::kv::KV;
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json, map::Entry};
use std::collections::HashMap;

const MESSAGES: &str = "messages";

#[derive(Debug, Serialize, Deserialize)]
struct LogEntry {
    offset: u64,
    value: Value,
    is_committed: bool,
}

pub struct Log<'a> {
    kv: KV<'a>,
}

impl<'a> Log<'a> {
    pub fn new(kv: KV<'a>) -> Self {
        Self { kv }
    }
    pub fn append(&self, key: &str, value: &Value) -> Result<u64> {
        let offset = loop {
            let default = serde_json::Map::new();
            let content = self.kv.read(MESSAGES).ok();
            let content = content
                .as_ref()
                .and_then(|v| v.as_object())
                .unwrap_or(&default);
            let mut new_content = content.clone();
            let mut offset = 0u64;
            for (_, entries) in new_content.iter() {
                if let Some(entries) = entries.as_array() {
                    offset += entries.len() as u64;
                };
            }
            let new_entry = LogEntry {
                offset,
                value: value.to_owned(),
                is_committed: false,
            };
            match new_content.entry(key) {
                Entry::Occupied(mut e) => {
                    e.get_mut()
                        .as_array_mut()
                        .ok_or(anyhow!("Content entries should be array"))?
                        .push(json!(new_entry));
                }
                Entry::Vacant(e) => {
                    e.insert(json!([new_entry]));
                }
            }
            match self.kv.compare_and_swap(MESSAGES, content, &new_content) {
                Ok(_) => break offset,
                Err(err) => eprintln!("retry {}", err),
            }
        };
        Ok(offset)
    }
    pub fn read(
        &self,
        key_to_offset: &HashMap<&str, u64>,
    ) -> Result<HashMap<String, Vec<(u64, Value)>>> {
        let mut result: HashMap<String, Vec<(u64, Value)>> = HashMap::new();
        let content = match self.kv.read(MESSAGES) {
            Ok(content) => content,
            Err(err) => {
                eprintln!("{}", err);
                return Ok(result);
            }
        };
        let content = content
            .as_object()
            .context(format!("{} value should be an object", MESSAGES))?;
        for (&key, &offset) in key_to_offset {
            let Some(entries) = content.get(key) else {
                continue;
            };
            let entries = entries
                .as_array()
                .context(format!("Key={}'s value should be an array", key))?;
            let mut values: Vec<(u64, Value)> = Vec::new();
            for entry_value in entries {
                let entry: LogEntry = serde_json::from_value(entry_value.to_owned())?;
                if entry.offset >= offset {
                    values.push((entry.offset, entry.value.clone()));
                }
            }
            if !values.is_empty() {
                result.insert(String::from(key), values);
            }
        }
        Ok(result)
    }
    pub fn commit(&self, key_to_offset: &HashMap<&str, u64>) -> Result<()> {
        loop {
            let content = self.kv.read(MESSAGES)?;
            let mut new_content = content.clone();
            for (&key, &offset) in key_to_offset {
                let Some(entries) = new_content.get_mut(key) else {
                    continue;
                };
                let entries = entries
                    .as_array_mut()
                    .context(format!("Key={}'s value should be an array", key))?;
                for entry_value in entries {
                    let mut entry: LogEntry = serde_json::from_value(entry_value.to_owned())?;
                    if entry.offset <= offset {
                        entry.is_committed = true;
                        *entry_value = json!(entry);
                    }
                }
            }
            match self.kv.compare_and_swap(MESSAGES, &content, &new_content) {
                Ok(_) => break,
                Err(err) => eprintln!("retry {}", err),
            }
        }
        Ok(())
    }
    pub fn read_committed(&self, keys: &[&str]) -> Result<HashMap<String, u64>> {
        let mut result: HashMap<String, u64> = HashMap::new();
        let content = match self.kv.read(MESSAGES) {
            Ok(content) => content,
            Err(err) => {
                eprintln!("{}", err);
                return Ok(result);
            }
        };
        let content = content
            .as_object()
            .context(format!("{} value should be an object", MESSAGES))?;
        for &key in keys {
            let Some(entries) = content.get(key) else {
                continue;
            };
            let entries = entries
                .as_array()
                .context(format!("Key={}'s value should be an array", key))?;
            for entry_value in entries.iter().rev() {
                let entry: LogEntry = serde_json::from_value(entry_value.to_owned())?;
                if entry.is_committed {
                    result.insert(String::from(key), entry.offset);
                }
            }
        }
        Ok(result)
    }
}
