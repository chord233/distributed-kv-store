# Distributed Key-Value Store

[中文文档](README_zh.md) | [English](README.md)

A high-performance, fault-tolerant distributed key-value store built with Rust, implementing the Raft consensus algorithm for strong consistency across multiple nodes.

## Features

### Core Features
- **Distributed Architecture**: Multi-node cluster with automatic leader election
- **Strong Consistency**: Raft consensus algorithm ensures data consistency across all nodes
- **Fault Tolerance**: Automatic failover and recovery from node failures
- **Persistent Storage**: Data persistence using the `sled` embedded database
- **High Performance**: Optimized for throughput and low latency

### API Support
- **HTTP REST API**: Easy-to-use RESTful interface
- **gRPC API**: High-performance binary protocol for client-server communication
- **Multiple Data Formats**: Support for JSON and binary data

### Operations
- **Key-Value Operations**: PUT, GET, DELETE, LIST
- **Cluster Management**: Add/remove nodes dynamically
- **Health Monitoring**: Health checks and metrics collection
- **Snapshot Support**: Efficient log compaction and state snapshots

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│     Node 1      │    │     Node 2      │    │     Node 3      │
│   (Leader)      │    │   (Follower)    │    │   (Follower)    │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│   HTTP/gRPC     │    │   HTTP/gRPC     │    │   HTTP/gRPC     │
│      API        │    │      API        │    │      API        │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│  Raft Consensus │◄──►│  Raft Consensus │◄──►│  Raft Consensus │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ Storage Engine  │    │ Storage Engine  │    │ Storage Engine  │
│    (Sled DB)    │    │    (Sled DB)    │    │    (Sled DB)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Quick Start

### Prerequisites

- Rust 1.70+ (with Cargo)
- Protocol Buffers compiler (`protoc`)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/your-username/distributed-kv-store.git
cd distributed-kv-store
```

2. Build the project:
```bash
cargo build --release
```

3. Run tests:
```bash
cargo test
```

### Running a Single Node

```bash
# Start a single node cluster
cargo run --bin kvstore-server -- \
  --node-id node1 \
  --bind-address 127.0.0.1:7001 \
  --http-port 8081 \
  --bootstrap
```

### Running a Multi-Node Cluster

1. Start the first node (bootstrap):
```bash
cargo run --bin kvstore-server -- \
  --node-id node1 \
  --bind-address 127.0.0.1:7001 \
  --http-port 8081 \
  --bootstrap
```

2. Start additional nodes:
```bash
# Node 2
cargo run --bin kvstore-server -- \
  --node-id node2 \
  --bind-address 127.0.0.1:7002 \
  --http-port 8082 \
  --peers 127.0.0.1:7001

# Node 3
cargo run --bin kvstore-server -- \
  --node-id node3 \
  --bind-address 127.0.0.1:7003 \
  --http-port 8083 \
  --peers 127.0.0.1:7001,127.0.0.1:7002
```

3. Add nodes to the cluster:
```bash
# Add node2 to the cluster
curl -X POST http://127.0.0.1:8081/api/v1/cluster/nodes \
  -H "Content-Type: application/json" \
  -d '{"node_id": "node2", "address": "127.0.0.1:7002"}'

# Add node3 to the cluster
curl -X POST http://127.0.0.1:8081/api/v1/cluster/nodes \
  -H "Content-Type: application/json" \
  -d '{"node_id": "node3", "address": "127.0.0.1:7003"}'
```

## Usage

### Using the Command Line Client

```bash
# Put a key-value pair
cargo run --bin kvstore-client -- put mykey "Hello, World!"

# Get a value
cargo run --bin kvstore-client -- get mykey

# List all keys
cargo run --bin kvstore-client -- list

# Delete a key
cargo run --bin kvstore-client -- delete mykey

# Check cluster status
cargo run --bin kvstore-client -- cluster-status

# Health check
cargo run --bin kvstore-client -- health
```

### Using HTTP API

#### Key-Value Operations

```bash
# PUT - Store a key-value pair
curl -X PUT http://127.0.0.1:8081/api/v1/kv/mykey \
  -H "Content-Type: application/json" \
  -d '{"key": "mykey", "value": "SGVsbG8sIFdvcmxkIQ=="}'

# GET - Retrieve a value
curl http://127.0.0.1:8081/api/v1/kv/mykey

# DELETE - Remove a key
curl -X DELETE http://127.0.0.1:8081/api/v1/kv/mykey

# LIST - Get all keys
curl http://127.0.0.1:8081/api/v1/kv
```

#### Cluster Management

```bash
# Get cluster status
curl http://127.0.0.1:8081/api/v1/cluster/status

# Add a node
curl -X POST http://127.0.0.1:8081/api/v1/cluster/nodes \
  -H "Content-Type: application/json" \
  -d '{"node_id": "node4", "address": "127.0.0.1:7004"}'

# Remove a node
curl -X DELETE http://127.0.0.1:8081/api/v1/cluster/nodes/node4

# Health check
curl http://127.0.0.1:8081/api/v1/health

# Get metrics
curl http://127.0.0.1:8081/api/v1/metrics
```

### Using gRPC API

The gRPC API provides the same functionality as the HTTP API but with better performance for high-throughput applications. See the protobuf definitions in `proto/` directory for the complete API specification.

## Configuration

### Server Configuration

The server can be configured via command line arguments or configuration files:

```bash
cargo run --bin kvstore-server -- --help
```

Key configuration options:
- `--node-id`: Unique identifier for the node
- `--bind-address`: Raft protocol binding address
- `--http-port`: HTTP API server port
- `--grpc-port`: gRPC API server port
- `--data-dir`: Data storage directory
- `--bootstrap`: Initialize as bootstrap node
- `--peers`: Comma-separated list of peer addresses

### Client Configuration

```bash
cargo run --bin kvstore-client -- --help
```

Client options:
- `--server`: Server address (default: http://127.0.0.1:8081)
- `--protocol`: Communication protocol (http/grpc)
- `--format`: Output format (json/table)
- `--timeout`: Request timeout



## Development

### Project Structure

```
src/
├── lib.rs              # Library root and common types
├── api/                # API layer
│   ├── mod.rs          # Core API service
│   ├── http.rs         # HTTP REST API server
│   └── grpc.rs         # gRPC API server
├── raft/               # Raft consensus implementation
│   ├── mod.rs          # Core Raft algorithm
│   ├── node.rs         # Node management
│   └── state_machine.rs # State machine
├── storage/            # Storage layer
│   ├── mod.rs          # Storage engine
│   ├── log.rs          # Raft log storage
│   └── snapshot.rs     # Snapshot management
├── network/            # Network communication
│   ├── mod.rs          # Network manager
│   ├── client.rs       # Network client
│   └── server.rs       # Network server
└── bin/                # Binary executables
    ├── kvstore-server.rs # Server binary
    └── kvstore-client.rs # Client binary
```

### Building from Source

1. Install dependencies:
```bash
# Ubuntu/Debian
sudo apt-get install protobuf-compiler

# macOS
brew install protobuf

# Arch Linux
sudo pacman -S protobuf
```

2. Build:
```bash
cargo build --release
```

3. Run tests:
```bash
cargo test
```

4. Run with logging:
```bash
RUST_LOG=info cargo run --bin kvstore-server
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

### Testing

```bash
# Run all tests
cargo test

# Run tests with output
cargo test -- --nocapture

# Run specific test module
cargo test storage::

# Run integration tests
cargo test --test integration

# Run with coverage (requires cargo-tarpaulin)
cargo tarpaulin --out Html
```

## Performance

### Benchmarks

Typical performance on modern hardware:

- **Throughput**: 10,000+ operations/second (single node)
- **Latency**: <5ms average (local network)
- **Cluster Size**: Tested up to 7 nodes
- **Data Size**: Supports values up to 64MB

### Optimization Tips

1. **Batch Operations**: Use bulk operations when possible
2. **Connection Pooling**: Reuse client connections
3. **Appropriate Cluster Size**: 3-5 nodes for most use cases
4. **SSD Storage**: Use SSDs for better I/O performance
5. **Network**: Low-latency network between nodes

## Monitoring

### Health Checks

```bash
# Basic health check
curl http://127.0.0.1:8081/api/v1/health

# Detailed metrics
curl http://127.0.0.1:8081/api/v1/metrics
```

### Metrics

The system exposes various metrics:
- Request latency and throughput
- Raft state and log information
- Storage usage and performance
- Network statistics
- Error rates and types

### Logging

Configure logging levels:
```bash
RUST_LOG=debug cargo run --bin kvstore-server
RUST_LOG=distributed_kv_store=info cargo run --bin kvstore-server
```

## Troubleshooting

### Common Issues

1. **Node won't start**:
   - Check if ports are available
   - Verify data directory permissions
   - Check peer addresses are reachable

2. **Cluster formation issues**:
   - Ensure bootstrap node is started first
   - Verify network connectivity between nodes
   - Check firewall settings

3. **Performance issues**:
   - Monitor disk I/O and network latency
   - Check for resource constraints
   - Review log compaction settings

4. **Data inconsistency**:
   - Verify cluster has a majority of nodes
   - Check for network partitions
   - Review Raft logs for errors

### Debug Mode

```bash
# Enable debug logging
RUST_LOG=debug cargo run --bin kvstore-server

# Enable trace logging for specific modules
RUST_LOG=distributed_kv_store::raft=trace cargo run --bin kvstore-server
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Raft Consensus Algorithm](https://raft.github.io/) by Diego Ongaro and John Ousterhout
- [Sled Database](https://github.com/spacejam/sled) for embedded storage
- [Tokio](https://tokio.rs/) for async runtime
- [Tonic](https://github.com/hyperium/tonic) for gRPC implementation

## Roadmap

- [ ] Multi-Raft support for horizontal scaling
- [ ] Read replicas for improved read performance
- [ ] Encryption at rest and in transit
- [ ] Web-based administration interface
- [ ] Kubernetes operator
- [ ] Cross-datacenter replication
- [ ] Backup and restore functionality
- [ ] Performance optimizations
- [ ] Additional client language bindings

## Support

For questions, issues, or contributions:
- Open an issue on GitHub
- Check the documentation
- Review existing issues and discussions

---

**Note**: This is a learning project implementing distributed systems concepts. While functional, it may not be suitable for production use without additional testing and hardening.