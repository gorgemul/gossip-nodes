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
git clone https://github.com/gorgemul/gossip-nodes.git
cd gossip-nodes
cargo build --release
```

## Test

```sh
# NOTE: Configure test_tool_dir and program_dir in the test.sh to real path, otherwise ./test.sh will throw errors like: "{test_tool_dir|program_dir} variable should be configured to real {maelstrom|program} directory"
# test_tool_dir="/my/own/maelstrom/dir"
# program_dir="/my/own/gossip-nodes/dir"
mv ./test.sh.example ./test.sh

# test {echo|unique-ids|single-node-broadcast|multi-nodes-broadcast|g-counter|kafka} workload
./test.sh
```
