use crate::kv::KV;
use crate::log;
use crate::message::{Message, MessageType};
use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::io::{self, Write};
use std::ops::Deref;
use std::sync::{Arc, Mutex, OnceLock, RwLock, mpsc};
use std::{env, fmt, thread};

const GLOBAL_COUNTER_KEY: &str = "g-counter-key";
const G_COUNTER_WORKLOAD: &str = "g-counter";
const BROADCAST_WORKLOAD: &str = "broadcast";
static WORKLOAD: OnceLock<String> = OnceLock::new();

// Handler use fn pointer since it is stateless
type Handler = fn(Arc<Node>, Message) -> Result<()>;
// Callback use closure since it need to capture the channel for sync rpc
type Callback = Box<dyn FnOnce(Arc<Node>, Message) -> Result<()> + Send + Sync>;

// Can only have one holder
struct NodeExclusiveState {
    callbacks: HashMap<u64, Callback>,
    messages: Vec<Value>,
    msg_id: u64,
}

// Multitple readers but one writer, will only be modifed when node init
#[derive(Debug)]
struct NodeSharedState {
    id: String,
    handlers: HashMap<MessageType, Handler>,
    neighbors: Vec<String>,
}

#[derive(Debug)]
pub struct Node {
    shared: RwLock<NodeSharedState>,
    exclusive: Mutex<NodeExclusiveState>,
}

impl Node {
    pub fn new() -> Result<Self> {
        let mut handlers: HashMap<MessageType, Handler> = HashMap::new();
        // register init handler
        handlers.insert(MessageType::Init, |node, request| {
            if node.is_init() {
                return Ok(());
            }
            let node_id = request.get_body_value("node_id", Value::as_str)?;
            {
                // Since reply need to take the reader lock, so we need to restrict the wrtier lock scope
                let mut shared = node.shared.write().unwrap();
                shared.id = node_id.to_owned();
                let neighbors = request.get_body_value("node_ids", Value::as_array)?;
                for (i, id) in neighbors.iter().enumerate() {
                    let id = id.as_str().context(format!(
                        "'node_ids' element {} is not a string type: {:?}",
                        i + 1,
                        id
                    ))?;
                    if id != node_id {
                        shared.neighbors.push(id.to_owned());
                    }
                }
            }
            node.reply(&request, HashMap::new())?;
            Ok(())
        });
        // register echo handler
        handlers.insert(MessageType::Echo, |node, request| {
            let body = request.body.extra.clone();
            node.reply(&request, body)?;
            Ok(())
        });
        // register generate handler
        handlers.insert(MessageType::Generate, |node, request| {
            let mut body = HashMap::new();
            body.insert(
                String::from("id"),
                json!(format!(
                    "{}#{}",
                    node.shared.read().unwrap().id,
                    node.msg_id()
                )),
            );
            node.reply(&request, body)?;
            Ok(())
        });
        // register topology handler
        // NOTE: only reply ok to the request, we get the neighbor info when init the node
        handlers.insert(MessageType::Topology, |node, request| {
            node.reply(&request, HashMap::new())?;
            Ok(())
        });
        // register broadcast handler
        handlers.insert(MessageType::Broadcast, |node, request| {
            let message = request.get_body_value_raw("message")?;
            {
                let messages = &mut node
                    .exclusive
                    .lock()
                    .expect("Fail to get lock in broadcast handler")
                    .messages;
                if messages.contains(message) {
                    node.reply(&request, HashMap::new())?;
                    return Ok(());
                }
                messages.push(message.to_owned());
            }
            for neighbor in &node.shared.read().unwrap().neighbors {
                if *neighbor == request.src {
                    continue;
                }
                let mut body: HashMap<String, Value> = HashMap::new();
                body.insert(
                    String::from("type"),
                    json!(MessageType::Broadcast.to_string()),
                );
                body.insert(String::from("message"), message.to_owned());
                node.rpc(neighbor, body, None)?;
            }
            node.reply(&request, HashMap::new())?;
            Ok(())
        });
        // NOTE: the broadcast workload rejects a 'value' key and the g-counter workload rejects
        // a 'messages' key, so the reply body only carries the field for the active workload.
        // register read handler
        handlers.insert(MessageType::Read, |node, request| {
            let mut body: HashMap<String, Value> = HashMap::new();
            let workload = WORKLOAD
                .get_or_init(|| env::var("WORKLOAD").unwrap_or(BROADCAST_WORKLOAD.to_owned()));
            match workload.deref() {
                // https://fly.io/dist-sys/4/  -> {"type":"read_ok","value":123}
                G_COUNTER_WORKLOAD => {
                    let seq_kv = KV::new_seq(&node);
                    let sync_key = format!("sync-{}", node.shared.read().unwrap().id);
                    let value = loop {
                        // seq-kv only guarantees sequential consistency: a request that does not change
                        // the store may be served from any state at or after the one this node last
                        // observed, so a plain read can return a counter that misses other nodes'
                        // already-acknowledged `add`s.
                        // The barrier is not "send a write", it is "actually change the store
                        if let Err(err) = seq_kv.write(&sync_key, node.msg_id()) {
                            eprintln!("retry: {}", err);
                            continue;
                        }
                        match seq_kv.read(GLOBAL_COUNTER_KEY) {
                            Ok(value) => break value.as_u64().unwrap_or(0),
                            Err(err) => eprintln!("retry: {}", err),
                        }
                    };
                    body.insert(String::from("value"), json!(value));
                }
                // Fallback to broadcast workload
                // https://fly.io/dist-sys/3a/ -> {"type":"read_ok","messages":[...]}
                _ => {
                    let messages = node
                        .exclusive
                        .lock()
                        .expect("Fail to get lock in read handler")
                        .messages
                        .clone();
                    body.insert(String::from("messages"), json!(messages));
                }
            }
            node.reply(&request, body)?;
            Ok(())
        });
        // register add handler
        handlers.insert(MessageType::Add, |node, request| {
            let delta = request.get_body_value("delta", Value::as_u64)?;
            let seq_kv = KV::new_seq(&node);
            loop {
                let value = seq_kv
                    .read(GLOBAL_COUNTER_KEY)
                    .ok()
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                match seq_kv.compare_and_swap(GLOBAL_COUNTER_KEY, value, value + delta) {
                    Ok(()) => break,
                    Err(err) => eprintln!("retry: {}", err),
                }
            }
            node.reply(&request, HashMap::new())?;
            Ok(())
        });
        // register send handler
        handlers.insert(MessageType::Send, |node, request| {
            let log = log::Log::new(KV::new_lin(&node));
            let offset = log.append(
                request.get_body_value("key", Value::as_str)?,
                request.get_body_value_raw("msg")?,
            )?;
            let mut body: HashMap<String, Value> = HashMap::new();
            body.insert(String::from("offset"), json!(offset));
            node.reply(&request, body)?;
            Ok(())
        });
        // register poll handler
        handlers.insert(MessageType::Poll, |node, request| {
            let key_to_offset = request.get_body_value("offsets", Value::as_object)?;
            let key_to_offset = key_to_offset
                .into_iter()
                .map(|(k, v)| Some((k.as_str(), v.as_u64()?)))
                .collect::<Option<HashMap<_, _>>>()
                .ok_or_else(|| anyhow!("Some offset value was not a u64"))?;
            let log = log::Log::new(KV::new_lin(&node));
            let messages = log.read(&key_to_offset)?;
            let mut body: HashMap<String, Value> = HashMap::new();
            body.insert(String::from("msgs"), json!(messages));
            node.reply(&request, body)?;
            Ok(())
        });
        // register commit offsets handler
        handlers.insert(MessageType::CommitOffsets, |node, request| {
            let key_to_offset = request.get_body_value("offsets", Value::as_object)?;
            let key_to_offset = key_to_offset
                .into_iter()
                .map(|(k, v)| Some((k.as_str(), v.as_u64()?)))
                .collect::<Option<HashMap<_, _>>>()
                .ok_or_else(|| anyhow!("Some offset value was not a u64"))?;
            let log = log::Log::new(KV::new_lin(&node));
            log.commit(&key_to_offset)?;
            node.reply(&request, HashMap::new())?;
            Ok(())
        });
        // register list committed offsets
        handlers.insert(MessageType::ListCommittedOffsets, |node, request| {
            let keys = request.get_body_value("keys", Value::as_array)?;
            let keys = keys
                .iter()
                .map(|v| v.as_str())
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| anyhow!("Some key value was not a string"))?;
            let log = log::Log::new(KV::new_lin(&node));
            let key_to_offset = log.read_committed(&keys)?;
            let mut body: HashMap<String, Value> = HashMap::new();
            body.insert(String::from("offsets"), json!(key_to_offset));
            node.reply(&request, body)?;
            Ok(())
        });
        let shared = RwLock::new(NodeSharedState {
            id: String::new(),
            neighbors: vec![],
            handlers,
        });
        let exclusive = Mutex::new(NodeExclusiveState {
            callbacks: HashMap::new(),
            msg_id: 0,
            messages: vec![],
        });
        Ok(Self { shared, exclusive })
    }
    fn is_init(&self) -> bool {
        !self.shared.read().unwrap().id.is_empty()
    }
    fn msg_id(&self) -> u64 {
        let mut state = self.exclusive.lock().expect("Fail to get lock in msg_id");
        let msg_id = state.msg_id;
        state.msg_id += 1;
        msg_id
    }
    fn rpc(
        &self,
        dest: &str,
        mut body: HashMap<String, Value>,
        callback: Option<Callback>,
    ) -> Result<()> {
        let msg_id = self.msg_id();
        body.insert(String::from("msg_id"), json!(msg_id));
        if let Some(callback) = callback {
            self.exclusive
                .lock()
                .expect("Fail to get lock in rpc")
                .callbacks
                .insert(msg_id, callback);
        }
        self.send(dest, body)?;
        Ok(())
    }
    pub fn rpc_sync(&self, dest: &str, body: HashMap<String, Value>) -> Result<Message> {
        let (tx, rx) = mpsc::channel::<Message>();
        self.rpc(
            dest,
            body,
            Some(Box::new(move |_, message| {
                tx.send(message).expect("Rpc sync send message fail");
                Ok(())
            })),
        )?;
        // TODO: maybe add a timeout for this, right now is waiting permanently
        let message = rx.recv()?;
        if let Some((code, text)) = message.is_rpc_error() {
            bail!("Rpc error: code={}, text={}", code, text);
        }
        Ok(message)
    }
    fn reply(&self, request: &Message, mut body: HashMap<String, Value>) -> Result<()> {
        let msg_id = request.get_body_value("msg_id", Value::as_u64)?;
        let response_type = format!("{}_ok", request.body.kind);
        body.entry(String::from("in_reply_to"))
            .and_modify(|v| *v = json!(msg_id))
            .or_insert(json!(msg_id));
        body.entry(String::from("type"))
            .and_modify(|v| *v = json!(response_type))
            .or_insert(json!(response_type));
        self.send(&request.src, body)?;
        Ok(())
    }
    fn send(&self, dest: &str, body: HashMap<String, Value>) -> Result<()> {
        let shared = self.shared.read().unwrap();
        let message = json!({
            "src": &shared.id,
            "dest": dest,
            "body": body,
        });
        let mut stdout = io::stdout().lock();
        eprintln!(
            "[{}] {} -> {}: {:?}",
            body.get("type").unwrap(),
            shared.id,
            dest,
            message.to_string()
        );
        writeln!(stdout, "{}", message)?;
        Ok(())
    }
    pub fn run(self: Arc<Self>) -> Result<()> {
        let requests = io::stdin().lines();
        for request in requests {
            let request = request.context("reading raw request from stdin fail")?;
            let request = Message::from_stdin(&request)?;
            eprintln!(
                "[{}] {} <- {}: {:?}",
                request.body.kind, request.dest, request.src, request.body.extra
            );
            // TODO: can do better
            while !self.is_init() && request.body.kind != MessageType::Init {
                thread::sleep(std::time::Duration::from_millis(100));
            }
            if let Ok(msg_id) = request.get_body_value("in_reply_to", Value::as_u64) {
                let Some(callback) = self
                    .exclusive
                    .lock()
                    .expect("Fail to get lock in run")
                    .callbacks
                    .remove(&msg_id)
                else {
                    continue;
                };
                let node = self.clone();
                thread::spawn(move || {
                    if let Err(err) = callback(node, request) {
                        eprintln!("Call back error: {:?}", err);
                    }
                });
                continue;
            }
            // Since thread::spawn need a handler which life time should be 'static, and
            // handler's life time is binding to self, so we need to make a copy of it
            let Some(handler) = self
                .shared
                .read()
                .unwrap()
                .handlers
                .get(&request.body.kind)
                .copied()
            else {
                eprintln!("Unsupported messsage type: {}", request.body.kind);
                continue;
            };
            let node = self.clone();
            thread::spawn(move || {
                if let Err(err) = handler(node, request) {
                    eprintln!("Handler error: {:?}", err);
                }
            });
        }
        Ok(())
    }
}

// Not use #[derive(Debug)] is because Callbacks doesn't have the default debug impl
impl fmt::Debug for NodeExclusiveState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeExclusiveState")
            .field("callbacks", &self.callbacks.keys().collect::<Vec<_>>())
            .field("messages", &self.messages)
            .field("msg_id", &self.msg_id)
            .finish()
    }
}
