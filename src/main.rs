use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json, to_string_pretty};
use std::collections::HashMap;
use std::io::{self, Write};

type Handler = fn(&mut Node, Message) -> Result<()>;

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

#[derive(Debug)]
struct Node {
    id: String,
    handlers: HashMap<MessageType, Handler>,
    callbacks: HashMap<u64, Handler>,
    neighbors: Vec<String>,
    messages: Vec<Value>,
    msg_id: u64,
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
            MessageType::Default(s) => s,
        };
        write!(f, "{}", s)
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
}

impl Node {
    fn new() -> Result<Self> {
        let mut handlers: HashMap<MessageType, Handler> = HashMap::new();
        // register init handler
        handlers.insert(MessageType::Init, |node, request| {
            if node.is_init() {
                // TODO: maybe return error?
                eprintln!("Init handler has been called before");
                return Ok(());
            }
            let node_id = request.get_body_value("node_id", Value::as_str)?;
            node.id = node_id.to_owned();
            let neighbors = request.get_body_value("node_ids", Value::as_array)?;
            for (i, id) in neighbors.iter().enumerate() {
                let id = id.as_str().context(format!(
                    "'node_ids' element {} is not a string type: {:?}",
                    i + 1,
                    id
                ))?;
                if id != node.id {
                    node.neighbors.push(id.to_owned());
                }
            }
            node.reply(request, HashMap::new())?;
            Ok(())
        });
        // register echo handler
        handlers.insert(MessageType::Echo, |node, request| {
            if !node.is_init() {
                eprintln!("Echo handler must be called after init handler");
                return Ok(());
            }
            let body = request.body.extra.clone();
            node.reply(request, body)?;
            Ok(())
        });
        // register generate handler
        handlers.insert(MessageType::Generate, |node, request| {
            if !node.is_init() {
                eprintln!("Generate handler must be called after init handler");
                return Ok(());
            }
            let mut body = HashMap::new();
            let msg_id = node.get_next_msg_id();
            body.insert(String::from("id"), json!(format!("{}#{}", node.id, msg_id)));
            node.reply(request, body)?;
            Ok(())
        });
        // register topology handler
        // NOTE: only reply ok to the request, we get the neighbor info when init the node
        handlers.insert(MessageType::Topology, |node, request| {
            if !node.is_init() {
                eprintln!("Topology handler must be called after init handler");
                return Ok(());
            }
            node.reply(request, HashMap::new())?;
            Ok(())
        });
        // register broadcast handler
        handlers.insert(MessageType::Broadcast, |node, request| {
            if !node.is_init() {
                eprintln!("broadcast handler must be called after init handler");
                return Ok(());
            }
            let message = request.get_body_value_raw("message")?;
            if node.messages.contains(message) {
                node.reply(request, HashMap::new())?;
                return Ok(());
            }
            node.messages.push(message.to_owned());
            for neighbor in node.neighbors.clone() {
                if neighbor == request.src {
                    continue;
                }
                let mut body: HashMap<String, Value> = HashMap::new();
                body.insert(String::from("type"), json!("broadcast"));
                body.insert(String::from("message"), message.to_owned());
                node.rpc(&neighbor, body, None)?;
            }
            node.reply(request, HashMap::new())?;
            Ok(())
        });
        // register read handler
        handlers.insert(MessageType::Read, |node, request| {
            if !node.is_init() {
                eprintln!("read handler must be called after init handler");
                return Ok(());
            }
            let mut body: HashMap<String, Value> = HashMap::new();
            body.insert(String::from("type"), json!("read"));
            body.insert(String::from("messages"), json!(node.messages.clone()));
            node.reply(request, body)?;
            Ok(())
        });
        Ok(Self {
            id: String::new(),
            handlers,
            callbacks: HashMap::new(),
            msg_id: 0,
            neighbors: vec![],
            messages: vec![],
        })
    }
    fn is_init(&self) -> bool {
        !self.id.is_empty()
    }
    fn get_next_msg_id(&mut self) -> u64 {
        self.msg_id += 1;
        self.msg_id
    }
    fn rpc(
        &mut self,
        dest: &str,
        mut body: HashMap<String, Value>,
        callback: Option<Handler>,
    ) -> Result<()> {
        let msg_id = self.get_next_msg_id();
        body.insert(String::from("msg_id"), json!(msg_id));
        if let Some(callback) = callback {
            self.callbacks.insert(msg_id, callback);
        }
        self.send(dest, body)?;
        Ok(())
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
            "'{}' send message to '{}':\n {}",
            self.id,
            dest,
            to_string_pretty(&message)?,
        );
        writeln!(stdout, "{}", message)?;
        Ok(())
    }
    fn run(&mut self) -> Result<()> {
        let requests = io::stdin().lines();
        for request in requests {
            let request = request.context("reading raw request from stdin fail")?;
            let request: Message = serde_json::from_str(&request)
                .context(format!("request is not valid json format: {}", request))?;
            if let Ok(msg_id) = request.get_body_value("in_reply_to", Value::as_u64) {
                let Some(callback) = self.callbacks.get(&msg_id) else {
                    continue;
                };
                callback(self, request)?;
                continue;
            }
            let Some(handler) = self.handlers.get(&request.body.kind) else {
                eprintln!("Unsupported messsage type: {}", request.body.kind);
                continue;
            };
            handler(self, request)?;
        }
        Ok(())
    }
}

fn main() {
    let mut node = Node::new().expect("Node init fail");
    if let Err(err) = node.run() {
        eprintln!("Node running error: {:?}", err);
    }
}
