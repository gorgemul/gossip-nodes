use serde_json::Value;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug)]
struct LogEntry {
    offset: u64,
    value: Value,
    is_committed: bool,
}

#[derive(Debug)]
pub struct Log {
    offset: Mutex<u64>,
    content: Mutex<HashMap<String, Vec<LogEntry>>>,
}

impl Log {
    pub fn new() -> Self {
        Log {
            offset: Mutex::new(0),
            content: Mutex::new(HashMap::new()),
        }
    }
    fn offset(&self) -> u64 {
        let mut offset = self.offset.lock().unwrap();
        let next_offset = *offset;
        *offset += 1;
        next_offset
    }
    pub fn append(&self, key: &str, value: &Value) -> u64 {
        let mut content = self.content.lock().unwrap();
        let offset = self.offset();
        let new_log_entry = LogEntry {
            offset,
            value: value.to_owned(),
            is_committed: false,
        };
        content
            .entry(String::from(key))
            .or_default()
            .push(new_log_entry);
        offset
    }
    pub fn read(&self, key_to_offset: &HashMap<&str, u64>) -> HashMap<String, Vec<(u64, Value)>> {
        let content = self.content.lock().unwrap();
        let mut result: HashMap<String, Vec<(u64, Value)>> = HashMap::new();
        for (&key, &offset) in key_to_offset {
            let Some(entries) = content.get(key) else {
                continue;
            };
            let mut values: Vec<(u64, Value)> = vec![];
            for entry in entries {
                if entry.offset >= offset {
                    values.push((entry.offset, entry.value.clone()));
                }
            }
            if !values.is_empty() {
                result.insert(String::from(key), values);
            }
        }
        result
    }
    pub fn commit(&self, key_to_offset: HashMap<&str, u64>) {
        let mut content = self.content.lock().unwrap();
        for (&key, &offset) in &key_to_offset {
            let Some(entries) = content.get_mut(key) else {
                continue;
            };
            for entry in entries {
                if entry.offset <= offset {
                    entry.is_committed = true;
                }
            }
        }
    }
    pub fn read_committed(&self, keys: &[&str]) -> HashMap<String, u64> {
        let content = self.content.lock().unwrap();
        let mut result: HashMap<String, u64> = HashMap::new();
        for &key in keys {
            let Some(entries) = content.get(key) else {
                continue;
            };
            for entry in entries.iter().rev() {
                if entry.is_committed {
                    result.insert(String::from(key), entry.offset);
                    break;
                }
            }
        }
        result
    }
}
