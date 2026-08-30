use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug)]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: MessageBody,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MessageBody {
    #[serde(rename = "type")]
    pub kind: MessageType,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum MessageType {
    Init,
    Echo,
    Error,
    Generate,
    Topology,
    Broadcast,
    Read,
    Add,
    #[serde(untagged)]
    Default(String),
}

#[repr(u64)]
enum RpcCode {
    Crash = 13,
}

// TODO: maybe have a new from reading the line
impl Message {
    pub fn from_stdin(message: &str) -> Result<Self> {
        serde_json::from_str(&message)
            .context(format!("request is not valid json format: {}", message))
    }
    pub fn get_body_value<'a, T>(
        &'a self,
        key: &str,
        to_fn: fn(&'a Value) -> Option<T>,
    ) -> Result<T> {
        let body = &self.body.extra;
        let value = body.get(key).context(format!(
            "Message body should contain '{}' field, message: {:?}",
            key, self
        ))?;
        let value = to_fn(value).context(format!(
            "'{}' value type expect to be '{}'",
            key,
            std::any::type_name::<T>()
        ))?;
        Ok(value)
    }
    pub fn get_body_value_raw(&self, key: &str) -> Result<&Value> {
        self.body.extra.get(key).context(format!(
            "Message body should contain '{}' field, message: {:?}",
            key, self
        ))
    }
    // None: indicate success
    // Some(code: u64, text: String) indicate error
    pub fn is_rpc_error(&self) -> Option<(u64, String)> {
        if self.body.kind != MessageType::Error {
            return None;
        }
        let code = match self.get_body_value("code", Value::as_u64) {
            Ok(code) => code,
            Err(err) => return Some((RpcCode::Crash as u64, err.to_string())),
        };
        if code == 0 {
            return None;
        }
        let text = match self.get_body_value("text", Value::as_str) {
            Ok(text) => text.to_owned(),
            Err(err) => return Some((RpcCode::Crash as u64, err.to_string())),
        };
        Some((code, text))
    }
}

impl std::fmt::Display for MessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            MessageType::Init => "init",
            MessageType::Echo => "echo",
            MessageType::Error => "error",
            MessageType::Generate => "generate",
            MessageType::Topology => "topology",
            MessageType::Broadcast => "broadcast",
            MessageType::Read => "read",
            MessageType::Add => "add",
            MessageType::Default(s) => s,
        };
        write!(f, "{}", s)
    }
}
