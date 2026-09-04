use crate::kv::KV;
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;

const CONTENT: &str = "content";

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
        loop {
            let content = self.kv.read(CONTENT).unwrap_or(json!({}));
            let offset: u64 = content
                .as_object()
                .unwrap_or(&serde_json::Map::new())
                .values()
                .filter_map(Value::as_array)
                .map(|entries| entries.len() as u64)
                .sum();
            let mut new_content = content.clone();
            // NOTE: Both unwrap is fine here since all are set default value
            new_content
                .as_object_mut()
                .unwrap()
                .entry(key)
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .unwrap()
                .push(json!(LogEntry {
                    offset,
                    value: value.to_owned(),
                    is_committed: false,
                }));
            let Err(err) = self.kv.compare_and_swap(CONTENT, &content, &new_content) else {
                return Ok(offset);
            };
            eprintln!("Log append retry: {}", err);
        }
    }
    pub fn read(
        &self,
        key_to_offset: &HashMap<&str, u64>,
    ) -> Result<HashMap<String, Vec<(u64, Value)>>> {
        let content = match self.kv.read(CONTENT) {
            Ok(content) => content,
            Err(err) => {
                eprintln!("{}", err);
                return Ok(HashMap::new());
            }
        };
        let Value::Object(mut content) = content else {
            bail!("{} value should be an object", CONTENT);
        };
        let mut result: HashMap<String, Vec<(u64, Value)>> = HashMap::new();
        for (&key, &offset) in key_to_offset {
            let Some(entries) = content.remove(key) else {
                continue;
            };
            let Value::Array(entries) = entries else {
                bail!("Key={} value should be an array", key);
            };
            let mut values: Vec<(u64, Value)> = Vec::new();
            for entry_value in entries {
                let entry: LogEntry = serde_json::from_value(entry_value)?;
                if entry.offset >= offset {
                    values.push((entry.offset, entry.value));
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
            let content = self.kv.read(CONTENT)?;
            let Value::Object(mut new_content) = content.clone() else {
                bail!("{CONTENT} value should be an object");
            };
            for (&key, &offset) in key_to_offset {
                let Some(entries) = new_content.get_mut(key) else {
                    continue;
                };
                let Value::Array(entries) = entries else {
                    bail!("Key={} value should be an array", key);
                };
                for entry_value in entries {
                    let mut entry: LogEntry = serde_json::from_value(entry_value.to_owned())?;
                    if entry.offset <= offset {
                        entry.is_committed = true;
                        *entry_value = json!(entry);
                    }
                }
            }
            match self
                .kv
                .compare_and_swap(CONTENT, &content, &json!(new_content))
            {
                Ok(_) => break,
                Err(err) => eprintln!("retry {}", err),
            }
        }
        Ok(())
    }
    pub fn read_committed(&self, keys: &[&str]) -> Result<HashMap<String, u64>> {
        let content = match self.kv.read(CONTENT) {
            Ok(content) => content,
            Err(err) => {
                eprintln!("{}", err);
                return Ok(HashMap::new());
            }
        };
        let Value::Object(mut content) = content else {
            bail!("{} value should be an object", CONTENT);
        };
        let mut result: HashMap<String, u64> = HashMap::new();
        for &key in keys {
            let Some(entries) = content.remove(key) else {
                continue;
            };
            let Value::Array(entries) = entries else {
                bail!("Key={} value should be an array", key);
            };
            for entry_value in entries.into_iter().rev() {
                let entry: LogEntry = serde_json::from_value(entry_value)?;
                if entry.is_committed {
                    result.insert(String::from(key), entry.offset);
                }
            }
        }
        Ok(result)
    }
}
