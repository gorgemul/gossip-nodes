use crate::node::Node;
use anyhow::Result;
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
enum KVType {
    Seq,
    Lin,
}

#[derive(Debug)]
pub struct KV<'a> {
    kind: KVType,
    node: &'a Arc<Node>,
}

impl<'a> KV<'a> {
    pub fn new_seq(node: &'a Arc<Node>) -> Self {
        Self {
            kind: KVType::Seq,
            node,
        }
    }
    pub fn new_lin(node: &'a Arc<Node>) -> Self {
        Self {
            kind: KVType::Lin,
            node,
        }
    }
    pub fn read(&self, key: &str) -> Result<Value> {
        let mut body: HashMap<String, Value> = HashMap::new();
        body.insert(String::from("type"), json!("read"));
        body.insert(String::from("key"), json!(key));
        let message = self.node.rpc_sync(&self.kind.to_string(), body)?;
        Ok(message.get_body_value_raw("value")?.to_owned())
    }
    pub fn write<T: Serialize>(&self, key: &str, value: T) -> Result<()> {
        let mut body: HashMap<String, Value> = HashMap::new();
        body.insert(String::from("type"), json!("write"));
        body.insert(String::from("key"), json!(key));
        body.insert(String::from("value"), json!(value));
        self.node.rpc_sync(&self.kind.to_string(), body)?;
        Ok(())
    }
    pub fn compare_and_swap<T: Serialize>(&self, key: &str, from: T, to: T) -> Result<()> {
        let mut body: HashMap<String, Value> = HashMap::new();
        body.insert(String::from("type"), json!("cas"));
        body.insert(String::from("key"), json!(key));
        body.insert(String::from("from"), json!(from));
        body.insert(String::from("to"), json!(to));
        body.insert(String::from("create_if_not_exists"), json!(true));
        self.node.rpc_sync(&self.kind.to_string(), body)?;
        Ok(())
    }
}

impl std::fmt::Display for KVType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            KVType::Seq => "seq-kv",
            KVType::Lin => "lin-kv",
        };
        write!(f, "{}", s)
    }
}
