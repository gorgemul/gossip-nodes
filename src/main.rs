mod kv;
mod log;
mod message;
mod node;
use std::sync::Arc;

fn main() {
    let node = Arc::new(node::Node::new().expect("Node init fail"));
    if let Err(err) = node.run() {
        eprintln!("Node running error: {:?}", err);
    }
    std::thread::sleep(std::time::Duration::from_millis(5000));
}
