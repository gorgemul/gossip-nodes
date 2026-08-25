# gossip-nodes

Rust implementations for the [Fly.io Distributed Systems Challenges](https://fly.io/dist-sys/).

## Dependencies

- **Rust**
- **[Maelstrom](https://github.com/jepsen-io/maelstrom/releases)** — the test harness
- **JDK** — required by Maelstrom
- **Graphviz** and **gnuplot** — required by Maelstrom for plots

On macOS:

```sh
brew install openjdk graphviz gnuplot
```

Then download a Maelstrom release tarball and extract it somewhere convenient.

## Build

```sh
cargo build --release
```

Binary lands at `target/release/gossip-nodes`.

## Run

Against the Maelstrom echo workload:

```sh
./maelstrom test -w echo --bin /path/to/gossip-nodes/target/release/gossip-nodes \
  --node-count 1 --time-limit 10
```

Against the Maelstrom unique id generation workload:

```sh
./maelstrom test -w unique-ids --bin /path/to/gossip-nodes/target/release/gossip-nodes \
--time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
```

Against the Maelstrom single node broadcast workload:

```sh
./maelstrom test -w broadcast --bin /path/to/gossip-nodes/target/release/gossip-nodes \ 
--node-count 1 --time-limit 20 --rate 10
```

Run it by hand by piping JSON lines into the binary:

```sh
echo '{"src":"c1","dest":"n1","body":{"type":"init","msg_id":1,"node_id":"n1","node_ids":["n1"]}}' \
  | cargo run
```
