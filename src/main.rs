use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::{Arc, Mutex, RwLock, mpsc};
use std::{fmt, thread};
mod kv;

// Handler use fn pointer since it is stateless
type Handler = fn(Arc<Node>, Message) -> Result<()>;
// Callback use closure since it need to capture the channel for sync rpc
type Callback = Box<dyn FnOnce(Arc<Node>, Message) -> Result<()> + Send + Sync>;

#[repr(u64)]
enum RpcCode {
    Crash = 13,
}

// Different maelstrom workloads expect a different `read_ok` shape, and each one rejects
// keys it doesn't know about, so the read handler has to pick exactly one of them.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Workload {
    // https://fly.io/dist-sys/3a/ -> {"type":"read_ok","messages":[...]}
    Broadcast,
    // https://fly.io/dist-sys/4/  -> {"type":"read_ok","value":123}
    Counter,
}

impl Workload {
    fn from_env() -> Self {
        match std::env::var("WORKLOAD").as_deref() {
            Ok("counter") | Ok("g-counter") => Workload::Counter,
            _ => Workload::Broadcast,
        }
    }
}

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Debug)]
#[serde(rename_all = "snake_case")]
enum MessageType {
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

#[derive(Serialize, Deserialize, Debug)]
struct MessageBody {
    #[serde(rename = "type")]
    kind: MessageType,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Message {
    src: String,
    dest: String,
    body: MessageBody,
}

struct NodeState {
    callbacks: HashMap<u64, Callback>,
    messages: Vec<Value>,
    msg_id: u64,
}

// TODO: maybe restrucutre the field, is kinda ugly right now
#[derive(Debug)]
pub struct Node {
    workload: Workload,
    id: RwLock<String>,
    handlers: RwLock<HashMap<MessageType, Handler>>,
    neighbors: RwLock<Vec<String>>,
    state: Mutex<NodeState>,
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

// Not use #[derive(Debug)] is because Callbacks doesn't have the default debug impl
impl fmt::Debug for NodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeState")
            .field("callbacks", &self.callbacks.keys().collect::<Vec<_>>())
            .field("messages", &self.messages)
            .field("msg_id", &self.msg_id)
            .finish()
    }
}

impl Message {
    fn get_body_value<'a, T>(&'a self, key: &str, to_fn: fn(&'a Value) -> Option<T>) -> Result<T> {
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
    fn get_body_value_raw(&self, key: &str) -> Result<&Value> {
        self.body.extra.get(key).context(format!(
            "Message body should contain '{}' field, message: {:?}",
            key, self
        ))
    }
    // None: indicate success
    // Some(code: u64, text: String) indicate error
    fn is_rpc_error(&self) -> Option<(u64, String)> {
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

impl Node {
    fn new(workload: Workload) -> Result<Self> {
        let mut handlers: HashMap<MessageType, Handler> = HashMap::new();
        // register init handler
        handlers.insert(MessageType::Init, |node, request| {
            if node.is_init() {
                return Ok(());
            }
            let node_id = request.get_body_value("node_id", Value::as_str)?;
            *node.id.write().unwrap() = node_id.to_owned();
            let neighbors = request.get_body_value("node_ids", Value::as_array)?;
            for (i, id) in neighbors.iter().enumerate() {
                let id = id.as_str().context(format!(
                    "'node_ids' element {} is not a string type: {:?}",
                    i + 1,
                    id
                ))?;
                if id != node_id {
                    (node.neighbors.write().unwrap()).push(id.to_owned());
                }
            }
            node.reply(request, HashMap::new())?;
            Ok(())
        });
        // register echo handler
        handlers.insert(MessageType::Echo, |node, request| {
            let body = request.body.extra.clone();
            node.reply(request, body)?;
            Ok(())
        });
        // register generate handler
        handlers.insert(MessageType::Generate, |node, request| {
            let mut body = HashMap::new();
            let msg_id = node.get_next_msg_id();
            body.insert(
                String::from("id"),
                json!(format!("{}#{}", node.id.read().unwrap(), msg_id)),
            );
            node.reply(request, body)?;
            Ok(())
        });
        // register topology handler
        // NOTE: only reply ok to the request, we get the neighbor info when init the node
        handlers.insert(MessageType::Topology, |node, request| {
            node.reply(request, HashMap::new())?;
            Ok(())
        });
        // register broadcast handler
        handlers.insert(MessageType::Broadcast, |node, request| {
            let message = request.get_body_value_raw("message")?;
            {
                let messages = &mut node
                    .state
                    .lock()
                    .expect("Fail to get lock in broadcast handler")
                    .messages;
                if messages.contains(message) {
                    node.reply(request, HashMap::new())?;
                    return Ok(());
                }
                messages.push(message.to_owned());
            }
            for neighbor in &*node.neighbors.read().unwrap() {
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
            node.reply(request, HashMap::new())?;
            Ok(())
        });
        // register read handler
        // NOTE: the broadcast workload rejects a 'value' key and the counter workload rejects
        // a 'messages' key, so the reply body only carries the field for the active workload.
        handlers.insert(MessageType::Read, |node, request| {
            let mut body: HashMap<String, Value> = HashMap::new();
            match node.workload {
                Workload::Broadcast => {
                    let messages = node
                        .state
                        .lock()
                        .expect("Fail to get lock in read handler")
                        .messages
                        .clone();
                    body.insert(String::from("messages"), json!(messages));
                }
                Workload::Counter => {
                    let value = loop {
                        match kv::seq_kv_read_u64_fresh(&node) {
                            Ok(value) => break value,
                            Err(err) => eprintln!("retry: {}", err),
                        }
                    };
                    body.insert(String::from("value"), json!(value));
                }
            }
            node.reply(request, body)?;
            Ok(())
        });
        // register add handler
        handlers.insert(MessageType::Add, |node, request| {
            let delta = request.get_body_value("delta", Value::as_u64)?;
            loop {
                let value = kv::seq_kv_read_u64(&node).unwrap_or(0);
                match kv::seq_kv_compare_and_swap_u64(&node, value, value + delta) {
                    Ok(()) => break,
                    Err(err) => eprintln!("retry: {}", err),
                }
            }
            node.reply(request, HashMap::new())?;
            Ok(())
        });
        let state = Mutex::new(NodeState {
            callbacks: HashMap::new(),
            msg_id: 0,
            messages: vec![],
        });
        Ok(Self {
            workload,
            id: RwLock::new(String::new()),
            neighbors: RwLock::new(vec![]),
            handlers: RwLock::new(handlers),
            state,
        })
    }
    fn is_init(&self) -> bool {
        !self
            .id
            .read()
            .expect("Fail to get read lock in is_init")
            .is_empty()
    }
    fn get_next_msg_id(&self) -> u64 {
        let mut state = self
            .state
            .lock()
            .expect("Fail to get lock in get_next_msg_id");
        state.msg_id += 1;
        state.msg_id
    }
    fn rpc(
        &self,
        dest: &str,
        mut body: HashMap<String, Value>,
        callback: Option<Callback>,
    ) -> Result<()> {
        let msg_id = self.get_next_msg_id();
        body.insert(String::from("msg_id"), json!(msg_id));
        if let Some(callback) = callback {
            self.state
                .lock()
                .expect("Fail to get lock in rpc")
                .callbacks
                .insert(msg_id, callback);
        }
        self.send(dest, body)?;
        Ok(())
    }
    fn rpc_sync(&self, dest: &str, body: HashMap<String, Value>) -> Result<Message> {
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
    fn reply(&self, request: Message, mut body: HashMap<String, Value>) -> Result<()> {
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
        let message = json!({
            "src": &self.id,
            "dest": dest,
            "body": body,
        });
        let mut stdout = io::stdout().lock();
        eprintln!(
            "[{}] {} -> {}: {:?}",
            body.get("type").unwrap(),
            self.id.read().unwrap(),
            dest,
            message.to_string()
        );
        writeln!(stdout, "{}", message)?;
        Ok(())
    }
    fn run(self: Arc<Self>) -> Result<()> {
        let requests = io::stdin().lines();
        for request in requests {
            let request = request.context("reading raw request from stdin fail")?;
            let request: Message = serde_json::from_str(&request)
                .context(format!("request is not valid json format: {}", request))?;
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
                    .state
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
                .handlers
                .read()
                .unwrap()
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

fn main() {
    let node = Arc::new(Node::new(Workload::from_env()).expect("Node init fail"));
    if let Err(err) = node.run() {
        eprintln!("Node running error: {:?}", err);
    }
}
