# 分布式键值存储系统

[中文文档](README_zh.md) | [English](README.md)

一个基于 Rust 构建的高性能、容错的分布式键值存储系统，实现了 Raft 共识算法以确保多节点间的强一致性。

## 功能特性

### 核心功能
- **分布式架构**: 多节点集群，支持自动领导者选举
- **强一致性**: Raft 共识算法确保所有节点间的数据一致性
- **容错能力**: 自动故障转移和节点故障恢复
- **持久化存储**: 使用 `sled` 嵌入式数据库进行数据持久化
- **高性能**: 针对吞吐量和低延迟进行优化

### API 支持
- **HTTP REST API**: 易于使用的 RESTful 接口
- **gRPC API**: 高性能的二进制协议，用于客户端-服务器通信
- **多种数据格式**: 支持 JSON 和二进制数据

### 操作功能
- **键值操作**: PUT、GET、DELETE、LIST
- **集群管理**: 动态添加/移除节点
- **健康监控**: 健康检查和指标收集
- **快照支持**: 高效的日志压缩和状态快照

## 系统架构

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│     节点 1      │    │     节点 2      │    │     节点 3      │
│   (领导者)      │    │   (跟随者)      │    │   (跟随者)      │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│   HTTP/gRPC     │    │   HTTP/gRPC     │    │   HTTP/gRPC     │
│      API        │    │      API        │    │      API        │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│  Raft 共识算法  │◄──►│  Raft 共识算法  │◄──►│  Raft 共识算法  │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│   存储引擎      │    │   存储引擎      │    │   存储引擎      │
│   (Sled DB)     │    │   (Sled DB)     │    │   (Sled DB)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 快速开始

### 环境要求

- Rust 1.70+ (包含 Cargo)
- Protocol Buffers 编译器 (`protoc`)

### 安装

1. 克隆仓库:
```bash
git clone https://github.com/your-username/distributed-kv-store.git
cd distributed-kv-store
```

2. 构建项目:
```bash
cargo build --release
```

3. 运行测试:
```bash
cargo test
```

### 运行单节点

```bash
# 启动单节点集群
cargo run --bin kvstore-server -- \
  --node-id node1 \
  --bind-address 127.0.0.1:7001 \
  --http-port 8081 \
  --bootstrap
```

### 运行多节点集群

1. 启动第一个节点（引导节点）:
```bash
cargo run --bin kvstore-server -- \
  --node-id node1 \
  --bind-address 127.0.0.1:7001 \
  --http-port 8081 \
  --bootstrap
```

2. 启动其他节点:
```bash
# 节点 2
cargo run --bin kvstore-server -- \
  --node-id node2 \
  --bind-address 127.0.0.1:7002 \
  --http-port 8082 \
  --peers 127.0.0.1:7001

# 节点 3
cargo run --bin kvstore-server -- \
  --node-id node3 \
  --bind-address 127.0.0.1:7003 \
  --http-port 8083 \
  --peers 127.0.0.1:7001,127.0.0.1:7002
```

3. 将节点添加到集群:
```bash
# 将 node2 添加到集群
curl -X POST http://127.0.0.1:8081/api/v1/cluster/nodes \
  -H "Content-Type: application/json" \
  -d '{"node_id": "node2", "address": "127.0.0.1:7002"}'

# 将 node3 添加到集群
curl -X POST http://127.0.0.1:8081/api/v1/cluster/nodes \
  -H "Content-Type: application/json" \
  -d '{"node_id": "node3", "address": "127.0.0.1:7003"}'
```

## 使用方法

### 使用命令行客户端

```bash
# 存储键值对
cargo run --bin kvstore-client -- put mykey "Hello, World!"

# 获取值
cargo run --bin kvstore-client -- get mykey

# 列出所有键
cargo run --bin kvstore-client -- list

# 删除键
cargo run --bin kvstore-client -- delete mykey

# 检查集群状态
cargo run --bin kvstore-client -- cluster-status

# 健康检查
cargo run --bin kvstore-client -- health
```

### 使用 HTTP API

#### 键值操作

```bash
# PUT - 存储键值对
curl -X PUT http://127.0.0.1:8081/api/v1/kv/mykey \
  -H "Content-Type: application/json" \
  -d '{"key": "mykey", "value": "SGVsbG8sIFdvcmxkIQ=="}'

# GET - 获取值
curl http://127.0.0.1:8081/api/v1/kv/mykey

# DELETE - 删除键
curl -X DELETE http://127.0.0.1:8081/api/v1/kv/mykey

# LIST - 获取所有键
curl http://127.0.0.1:8081/api/v1/kv
```

#### 集群管理

```bash
# 获取集群状态
curl http://127.0.0.1:8081/api/v1/cluster/status

# 添加节点
curl -X POST http://127.0.0.1:8081/api/v1/cluster/nodes \
  -H "Content-Type: application/json" \
  -d '{"node_id": "node4", "address": "127.0.0.1:7004"}'

# 移除节点
curl -X DELETE http://127.0.0.1:8081/api/v1/cluster/nodes/node4

# 健康检查
curl http://127.0.0.1:8081/api/v1/health

# 获取指标
curl http://127.0.0.1:8081/api/v1/metrics
```

### 使用 gRPC API

gRPC API 提供与 HTTP API 相同的功能，但在高吞吐量应用中具有更好的性能。完整的 API 规范请参见 `proto/` 目录中的 protobuf 定义。

## 配置

### 服务器配置

服务器可以通过命令行参数或配置文件进行配置:

```bash
cargo run --bin kvstore-server -- --help
```

主要配置选项:
- `--node-id`: 节点的唯一标识符
- `--bind-address`: Raft 协议绑定地址
- `--http-port`: HTTP API 服务器端口
- `--grpc-port`: gRPC API 服务器端口
- `--data-dir`: 数据存储目录
- `--bootstrap`: 初始化为引导节点
- `--peers`: 逗号分隔的对等节点地址列表

### 客户端配置

```bash
cargo run --bin kvstore-client -- --help
```

客户端选项:
- `--server`: 服务器地址 (默认: http://127.0.0.1:8081)
- `--protocol`: 通信协议 (http/grpc)
- `--format`: 输出格式 (json/table)
- `--timeout`: 请求超时时间

## 开发

### 项目结构

```
src/
├── lib.rs              # 库根文件和通用类型
├── api/                # API 层
│   ├── mod.rs          # 核心 API 服务
│   ├── http.rs         # HTTP REST API 服务器
│   └── grpc.rs         # gRPC API 服务器
├── raft/               # Raft 共识算法实现
│   ├── mod.rs          # 核心 Raft 算法
│   ├── node.rs         # 节点管理
│   └── state_machine.rs # 状态机
├── storage/            # 存储层
│   ├── mod.rs          # 存储引擎
│   ├── log.rs          # Raft 日志存储
│   └── snapshot.rs     # 快照管理
├── network/            # 网络通信
│   ├── mod.rs          # 网络管理器
│   ├── client.rs       # 网络客户端
│   └── server.rs       # 网络服务器
└── bin/                # 可执行文件
    ├── kvstore-server.rs # 服务器二进制文件
    └── kvstore-client.rs # 客户端二进制文件
```

### 从源码构建

1. 安装依赖:
```bash
# Ubuntu/Debian
sudo apt-get install protobuf-compiler

# macOS
brew install protobuf

# Arch Linux
sudo pacman -S protobuf
```

2. 构建:
```bash
cargo build --release
```

3. 运行测试:
```bash
cargo test
```

4. 带日志运行:
```bash
RUST_LOG=info cargo run --bin kvstore-server
```

### 贡献

1. Fork 仓库
2. 创建功能分支
3. 进行更改
4. 为新功能添加测试
5. 确保所有测试通过
6. 提交 Pull Request

### 测试

```bash
# 运行所有测试
cargo test

# 运行测试并显示输出
cargo test -- --nocapture

# 运行特定测试模块
cargo test storage::

# 运行集成测试
cargo test --test integration

# 运行覆盖率测试 (需要 cargo-tarpaulin)
cargo tarpaulin --out Html
```

## 性能

### 基准测试

在现代硬件上的典型性能:

- **吞吐量**: 10,000+ 操作/秒 (单节点)
- **延迟**: <5ms 平均延迟 (本地网络)
- **集群规模**: 已测试最多 7 个节点
- **数据大小**: 支持最大 64MB 的值

### 优化建议

1. **批量操作**: 尽可能使用批量操作
2. **连接池**: 重用客户端连接
3. **适当的集群大小**: 大多数用例建议 3-5 个节点
4. **SSD 存储**: 使用 SSD 获得更好的 I/O 性能
5. **网络**: 节点间使用低延迟网络

## 监控

### 健康检查

```bash
# 基本健康检查
curl http://127.0.0.1:8081/api/v1/health

# 详细指标
curl http://127.0.0.1:8081/api/v1/metrics
```

### 指标

系统暴露各种指标:
- 请求延迟和吞吐量
- Raft 状态和日志信息
- 存储使用情况和性能
- 网络统计
- 错误率和类型

### 日志

配置日志级别:
```bash
RUST_LOG=debug cargo run --bin kvstore-server
RUST_LOG=distributed_kv_store=info cargo run --bin kvstore-server
```

## 故障排除

### 常见问题

1. **节点无法启动**:
   - 检查端口是否可用
   - 验证数据目录权限
   - 检查对等节点地址是否可达

2. **集群组建问题**:
   - 确保引导节点首先启动
   - 验证节点间网络连接
   - 检查防火墙设置

3. **性能问题**:
   - 监控磁盘 I/O 和网络延迟
   - 检查资源约束
   - 审查日志压缩设置

4. **数据不一致**:
   - 验证集群有大多数节点
   - 检查网络分区
   - 审查 Raft 日志错误

### 调试模式

```bash
# 启用调试日志
RUST_LOG=debug cargo run --bin kvstore-server

# 为特定模块启用跟踪日志
RUST_LOG=distributed_kv_store::raft=trace cargo run --bin kvstore-server
```

## 许可证

本项目采用 MIT 许可证 - 详情请参见 [LICENSE](LICENSE) 文件。

## 致谢

- [Raft 共识算法](https://raft.github.io/) by Diego Ongaro 和 John Ousterhout
- [Sled 数据库](https://github.com/spacejam/sled) 用于嵌入式存储
- [Tokio](https://tokio.rs/) 用于异步运行时
- [Tonic](https://github.com/hyperium/tonic) 用于 gRPC 实现

## 路线图

- [ ] Multi-Raft 支持，用于水平扩展
- [ ] 只读副本，提高读取性能
- [ ] 静态和传输加密
- [ ] 基于 Web 的管理界面
- [ ] Kubernetes 操作器
- [ ] 跨数据中心复制
- [ ] 备份和恢复功能
- [ ] 性能优化
- [ ] 其他客户端语言绑定

## 支持

如有问题、问题或贡献:
- 在 GitHub 上开启 issue
- 查看文档
- 审查现有的 issues 和讨论

---

**注意**: 这是一个学习分布式系统概念的项目。虽然功能完整，但在生产环境使用前可能需要额外的测试和加固。