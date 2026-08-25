use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json, to_string_pretty};
use std::collections::HashMap;
use std::io::{self, Write};

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
    neighbors: Vec<String>,
    messages: Vec<Value>,
    counter: u64,
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
    fn new() -> Self {
        Self {
            id: String::new(),
            counter: 0,
            neighbors: vec![],
            messages: vec![],
        }
    }
    fn is_init(&self) -> bool {
        !self.id.is_empty()
    }
    fn init_handler(&mut self, request: Message) -> Result<()> {
        if self.is_init() {
            // TODO: maybe return error?
            eprintln!("Init handler has been called before");
            return Ok(());
        }
        let node_id = request.get_body_value("node_id", Value::as_str)?;
        self.id = node_id.to_owned();
        let neighbors = request.get_body_value("node_ids", Value::as_array)?;
        for (i, id) in neighbors.iter().enumerate() {
            let id = id.as_str().context(format!(
                "'node_ids' element {} is not a string type: {:?}",
                i + 1,
                id
            ))?;
            if id != self.id {
                self.neighbors.push(id.to_owned());
            }
        }
        self.reply(request, HashMap::new())?;
        eprintln!("Init node successs: {:?}", self);
        Ok(())
    }
    fn echo_handler(&self, request: Message) -> Result<()> {
        if !self.is_init() {
            eprintln!("Echo handler must be called after init handler");
            return Ok(());
        }
        let body = request.body.extra.clone();
        self.reply(request, body)?;
        Ok(())
    }
    fn generate_handler(&mut self, request: Message) -> Result<()> {
        if !self.is_init() {
            eprintln!("Generate handler must be called after init handler");
            return Ok(());
        }
        let mut body = HashMap::new();
        body.insert(
            String::from("id"),
            json!(format!("{}#{}", self.id, self.counter)),
        );
        self.counter += 1;
        self.reply(request, body)?;
        Ok(())
    }
    // NOTE: only reply ok to the request, we get the neighbor info when init the node
    fn topology_handler(&self, request: Message) -> Result<()> {
        if !self.is_init() {
            eprintln!("Topology handler must be called after init handler");
            return Ok(());
        }
        self.reply(request, HashMap::new())?;
        Ok(())
    }
    fn broadcast_handler(&mut self, request: Message) -> Result<()> {
        if !self.is_init() {
            eprintln!("broadcast handler must be called after init handler");
            return Ok(());
        }
        let message = request.get_body_value_raw("message")?;
        self.messages.push(message.to_owned());
        for neighbor in &self.neighbors {
            if *neighbor == request.src {
                continue;
            }
            let mut body: HashMap<String, Value> = HashMap::new();
            body.insert(String::from("type"), json!("broadcast"));
            body.insert(String::from("message"), message.to_owned());
            self.send(neighbor, body)?;
        }
        self.reply(request, HashMap::new())?;
        Ok(())
    }
    fn read_handler(&self, request: Message) -> Result<()> {
        if !self.is_init() {
            eprintln!("read handler must be called after init handler");
            return Ok(());
        }
        let mut body: HashMap<String, Value> = HashMap::new();
        body.insert(String::from("type"), json!("read"));
        body.insert(String::from("messages"), json!(self.messages.clone()));
        self.reply(request, body)?;
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
        let lines = io::stdin().lines();
        for line in lines {
            let line = line.context("reading line from stdin fail")?;
            let message: Message = serde_json::from_str(&line)
                .context(format!("message is not valid json format: {}", line))?;
            match message.body.kind {
                MessageType::Init => self.init_handler(message)?,
                MessageType::Echo => self.echo_handler(message)?,
                MessageType::Error => unimplemented!(),
                MessageType::Generate => self.generate_handler(message)?,
                MessageType::Topology => self.topology_handler(message)?,
                MessageType::Broadcast => self.broadcast_handler(message)?,
                MessageType::Read => self.read_handler(message)?,
                MessageType::Default(v) => eprintln!("Unsupported messsage type: {}", v),
            }
        }
        Ok(())
    }
}

fn main() {
    let mut node = Node::new();
    if let Err(err) = node.run() {
        eprintln!("Echo node running error: {:?}", err);
    }
}
