use crate::node::Node;
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

static SEQ_KV_COUNTER: AtomicU64 = AtomicU64::new(0);

const SEQ_KV: &str = "seq-kv";
const GLOBAL_COUNTER_KEY: &str = "g-counter-key";

pub fn seq_kv_read_u64(node: &Arc<Node>) -> Result<u64> {
    let mut body: HashMap<String, Value> = HashMap::new();
    body.insert(String::from("type"), json!("read"));
    body.insert(String::from("key"), json!(GLOBAL_COUNTER_KEY));
    let message = node
        .rpc_sync(SEQ_KV, body)
        .context("seq_kv_read_u64 fail:")?;
    message.get_body_value("value", Value::as_u64)
}

pub fn seq_kv_write_u64(node: &Arc<Node>, key: &str, value: u64) -> Result<()> {
    let mut body: HashMap<String, Value> = HashMap::new();
    body.insert(String::from("type"), json!("write"));
    body.insert(String::from("key"), json!(key));
    body.insert(String::from("value"), json!(value));
    node.rpc_sync(SEQ_KV, body)
        .context("seq_kv_write_u64 fail:")?;
    Ok(())
}

// seq-kv only guarantees sequential consistency: a request that does not change
// the store may be served from any state at or after the one this node last
// observed, so a plain read can return a counter that misses other nodes'
// already-acknowledged `add`s. key content don't matter for barrier.
// But must be a write (state-changing op), because barrier come from write ordering,
// not from key identity. For better debugging can have a key with node_id
pub fn seq_kv_read_u64_fresh(node: &Arc<Node>) -> Result<u64> {
    seq_kv_write_u64(node, "sync", SEQ_KV_COUNTER.fetch_add(1, Ordering::SeqCst))?;
    seq_kv_read_u64(node)
}

pub fn seq_kv_compare_and_swap_u64(node: &Arc<Node>, from: u64, to: u64) -> Result<()> {
    let mut body: HashMap<String, Value> = HashMap::new();
    body.insert(String::from("type"), json!("cas"));
    body.insert(String::from("key"), json!(GLOBAL_COUNTER_KEY));
    body.insert(String::from("from"), json!(from));
    body.insert(String::from("to"), json!(to));
    body.insert(String::from("create_if_not_exists"), json!(true));
    node.rpc_sync(SEQ_KV, body)
        .context("seq_kv_compare_and_swap fail:")?;
    Ok(())
}
