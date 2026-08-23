use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json, to_string_pretty};
use std::collections::HashMap;
use std::io::{self, Write};

#[derive(Serialize, Deserialize, Debug, Hash, PartialEq, Eq)]
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

impl Node {
    fn new() -> Self {
        Self {
            id: String::new(),
            node_ids: vec![],
        }
    }
    fn init_handler(&mut self, message: Message) -> Result<()> {
        if !self.id.is_empty() {
            eprintln!("Init handler has been called before");
            return Ok(());
        }
        let body = message.body.extra;
        let node_id = body
            .get("node_id")
            .context(format!(
                "Init message body should contain 'node_id' filed, body: {:?}",
                body
            ))?
            .as_str()
            .context(format!("'node_id' should be a string, body: {:?}", body))?;
        self.id = node_id.to_owned();
        let node_ids = body
            .get("node_ids")
            .context(format!(
                "Init message body should contain 'node_ids' field, body: {:?}",
                body
            ))?
            .as_array()
            .context(format!("'node_ids' should be an array, body: {:?}", body))?;
        for (i, id) in node_ids.iter().enumerate() {
            let id = id.as_str().context(format!(
                "'node_ids' element {} is not a string type: {:?}",
                i + 1,
                id
            ))?;
            self.node_ids.push(id.to_owned());
        }
        let msg_id = body
            .get("msg_id")
            .context(format!(
                "Echo message body should contain 'msg_id' field, body: {:?}",
                body
            ))?
            .as_u64()
            .context("'msg_id' should be an integer")?;
        let mut body: HashMap<String, Value> = HashMap::new();
        body.insert(String::from("in_reply_to"), json!(msg_id));
        self.send(&message.src, "init_ok", body)?;
        eprintln!("Init node successs: {:?}", self);
        Ok(())
    }
    fn echo_handler(&self, message: Message) -> Result<()> {
        let mut body = message.body.extra;
        let msg_id = body
            .get("msg_id")
            .context(format!(
                "Echo message body should contain 'msg_id' field, body: {:?}",
                body
            ))?
            .as_u64()
            .context("'msg_id' should be an integer")?;
        body.entry(String::from("in_reply_to"))
            .and_modify(|e| *e = json!(msg_id))
            .or_insert(json!(msg_id));
        self.send(&message.src, "echo_ok", body)?;
        Ok(())
    }
    // Kind is a string here, which could be anything that current node defined, we only restrict received message type.
    fn send(&self, dest: &str, kind: &str, body: HashMap<String, Value>) -> Result<()> {
        let mut message = json!({
            "src": &self.id,
            "dest": dest,
            "body": {
                "type": kind,
            },
        });
        for (k, v) in body.into_iter() {
            message["body"][k] = v;
        }
        let mut stdout = io::stdout().lock();
        eprintln!(
            "'{}' send message to '{}':\n {}",
            self.id,
            dest,
            to_string_pretty(&message)?,
        );
        writeln!(stdout, "{}", message.to_string())?;
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
