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
    node_ids: Vec<String>,
}

impl std::fmt::Display for MessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            MessageType::Init => "init",
            MessageType::Echo => "echo",
            MessageType::Error => "error",
            MessageType::Default(s) => s,
        };
        write!(f, "{}", s)
    }
}

impl MessageBody {
    fn get_value<'a, T>(&'a self, key: &str, to_fn: fn(&'a Value) -> Option<T>) -> Result<T> {
        let body = &self.extra;
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
}

impl Node {
    fn new() -> Self {
        Self {
            id: String::new(),
            node_ids: vec![],
        }
    }
    fn init_handler(&mut self, request: Message) -> Result<()> {
        if !self.id.is_empty() {
            eprintln!("Init handler has been called before");
            return Ok(());
        }
        let node_id = request.body.get_value("node_id", Value::as_str)?;
        self.id = node_id.to_owned();
        let node_ids = request.body.get_value("node_ids", Value::as_array)?;
        for (i, id) in node_ids.iter().enumerate() {
            let id = id.as_str().context(format!(
                "'node_ids' element {} is not a string type: {:?}",
                i + 1,
                id
            ))?;
            self.node_ids.push(id.to_owned());
        }
        self.reply(request, HashMap::new())?;
        eprintln!("Init node successs: {:?}", self);
        Ok(())
    }
    fn echo_handler(&self, request: Message) -> Result<()> {
        let body = request.body.extra.clone();
        self.reply(request, body)?;
        Ok(())
    }
    fn reply(&self, request: Message, mut body: HashMap<String, Value>) -> Result<()> {
        let msg_id = request.body.get_value("msg_id", Value::as_u64)?;
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
