//! # 分布式键值存储系统
//!
//! 这是一个基于 Raft 共识算法的高性能分布式键值存储系统。该系统提供强一致性保证，
//! 支持多节点集群部署，具有自动故障恢复和动态扩缩容能力。
//!
//! ## 核心特性
//!
//! - **强一致性**: 基于 Raft 算法确保所有节点数据一致
//! - **高可用性**: 支持节点故障自动恢复和领导者选举
//! - **水平扩展**: 支持动态添加和移除集群节点
//! - **持久化存储**: 基于 sled 嵌入式数据库的高性能存储
//! - **多协议支持**: 提供 HTTP REST API 和 gRPC 接口
//! - **快照机制**: 支持日志压缩和快速状态恢复
//!
//! ## 架构组件
//!
//! ### 核心模块
//!
//! - [`raft`]: Raft 共识算法实现，包括领导者选举和日志复制
//! - [`storage`]: 持久化存储层，管理数据、日志和快照
//! - [`network`]: 网络通信层，处理节点间的 Raft 协议通信
//! - [`api`]: API 服务层，提供 HTTP 和 gRPC 接口
//!
//! ### 数据流
//!
//! ```text
//! 客户端请求 → API 层 → Raft 共识 → 存储层 → 持久化
//!      ↑                                    ↓
//!   响应结果 ← 状态机应用 ← 日志提交 ← 日志复制
//! ```
//!
//! ## 快速开始
//!
//! ### 启动单节点集群
//!
//! ```rust
//! use distributed_kv_store::{
//!     NodeConfig, RaftConfig,
//!     api::ApiService,
//!     raft::RaftConsensus,
//!     storage::Storage,
//! };
//! use std::collections::HashMap;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // 配置节点
//!     let config = NodeConfig {
//!         node_id: "node1".to_string(),
//!         listen_addr: "127.0.0.1:8080".to_string(),
//!         grpc_port: 50051,
//!         http_port: 3000,
//!         data_dir: "./data/node1".to_string(),
//!         peers: HashMap::new(),
//!         raft_config: RaftConfig::default(),
//!     };
//!
//!     // 创建存储
//!     let storage = Storage::new(&config.data_dir).await?;
//!
//!     // 启动 Raft 共识
//!     let raft = RaftConsensus::new(config.clone(), storage).await?;
//!     raft.start().await?;
//!
//!     // 启动 API 服务
//!     let api_service = ApiService::new(raft);
//!     api_service.start_http(config.http_port).await?;
//!     api_service.start_grpc(config.grpc_port).await?;
//!
//!     println!("键值存储服务已启动");
//!     tokio::signal::ctrl_c().await?;
//!     Ok(())
//! }
//! ```
//!
//! ### 客户端使用示例
//!
//! ```rust
//! use reqwest;
//! use serde_json::json;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = reqwest::Client::new();
//!     let base_url = "http://localhost:3000/api/v1";
//!
//!     // 设置键值对
//!     let response = client
//!         .put(&format!("{}/kv/user:1", base_url))
//!         .json(&json!({"value": "Alice"}))
//!         .send()
//!         .await?;
//!
//!     println!("PUT 响应: {}", response.status());
//!
//!     // 获取值
//!     let response = client
//!         .get(&format!("{}/kv/user:1", base_url))
//!         .send()
//!         .await?;
//!
//!     let value: serde_json::Value = response.json().await?;
//!     println!("GET 响应: {:?}", value);
//!
//!     Ok(())
//! }
//! ```
//!
//! ## 集群部署
//!
//! ### 三节点集群示例
//!
//! ```bash
//! # 节点 1
//! cargo run --bin kvstore-server -- \
//!   --node-id node1 \
//!   --address 127.0.0.1:8080 \
//!   --http-port 3000 \
//!   --grpc-port 50051 \
//!   --peers node2=127.0.0.1:8081,node3=127.0.0.1:8082
//!
//! # 节点 2
//! cargo run --bin kvstore-server -- \
//!   --node-id node2 \
//!   --address 127.0.0.1:8081 \
//!   --http-port 3001 \
//!   --grpc-port 50052 \
//!   --peers node1=127.0.0.1:8080,node3=127.0.0.1:8082
//!
//! # 节点 3
//! cargo run --bin kvstore-server -- \
//!   --node-id node3 \
//!   --address 127.0.0.1:8082 \
//!   --http-port 3002 \
//!   --grpc-port 50053 \
//!   --peers node1=127.0.0.1:8080,node2=127.0.0.1:8081
//! ```
//!
//! ## 性能特点
//!
//! - **高吞吐量**: 支持每秒数万次读写操作
//! - **低延迟**: 毫秒级响应时间
//! - **内存效率**: 优化的内存使用和垃圾回收
//! - **网络优化**: 批量操作和连接复用
//!
//! ## 一致性保证
//!
//! - **线性一致性**: 所有操作按全局顺序执行
//! - **持久性**: 已确认的写操作不会丢失
//! - **分区容错**: 网络分区时保持可用性
//! - **最终一致性**: 分区恢复后自动同步

pub mod api;
pub mod network;
pub mod raft;
pub mod storage;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Node identifier type
pub type NodeId = String;

/// Term number in Raft algorithm
pub type Term = u64;

/// Log index in Raft algorithm
pub type LogIndex = u64;

/// Configuration for a distributed KV store node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Unique identifier for this node
    pub node_id: NodeId,
    /// Address this node listens on
    pub listen_addr: String,
    /// Port for gRPC API
    pub grpc_port: u16,
    /// Port for HTTP API
    pub http_port: u16,
    /// Data directory for persistent storage
    pub data_dir: String,
    /// Initial cluster peers
    pub peers: HashMap<NodeId, String>,
    /// Raft configuration
    pub raft_config: RaftConfig,
}

/// Raft algorithm configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    /// Election timeout in milliseconds
    pub election_timeout_ms: u64,
    /// Heartbeat interval in milliseconds
    pub heartbeat_interval_ms: u64,
    /// Maximum entries per append request
    pub max_entries_per_request: usize,
    /// Snapshot threshold (log entries)
    pub snapshot_threshold: usize,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_ms: 150,
            heartbeat_interval_ms: 50,
            max_entries_per_request: 100,
            snapshot_threshold: 1000,
        }
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        let node_id = Uuid::new_v4().to_string();
        Self {
            node_id,
            listen_addr: "127.0.0.1".to_string(),
            grpc_port: 8080,
            http_port: 8081,
            data_dir: "./data".to_string(),
            peers: HashMap::new(),
            raft_config: RaftConfig::default(),
        }
    }
}

/// Error types for the distributed KV store
#[derive(thiserror::Error, Debug)]
pub enum KvStoreError {
    #[error("Storage error: {0}")]
    Storage(#[from] storage::StorageError),
    
    #[error("Raft error: {0}")]
    Raft(#[from] raft::RaftError),
    
    #[error("Network error: {0}")]
    Network(#[from] network::NetworkError),
    
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    
    #[error("Key not found: {0}")]
    KeyNotFound(String),
    
    #[error("Node not leader")]
    NotLeader,
    
    #[error("Cluster not ready")]
    ClusterNotReady,
    
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
}

pub type KvResult<T> = Result<T, KvStoreError>;

/// Include generated protobuf code
pub mod proto {
    tonic::include_proto!("kvstore");
    
    /// File descriptor set for reflection
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("kvstore_descriptor");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = NodeConfig::default();
        assert!(!config.node_id.is_empty());
        assert_eq!(config.listen_addr, "127.0.0.1");
        assert_eq!(config.grpc_port, 8080);
        assert_eq!(config.http_port, 8081);
    }

    #[test]
    fn test_raft_config() {
        let raft_config = RaftConfig::default();
        assert_eq!(raft_config.election_timeout_ms, 150);
        assert_eq!(raft_config.heartbeat_interval_ms, 50);
        assert_eq!(raft_config.max_entries_per_request, 100);
        assert_eq!(raft_config.snapshot_threshold, 1000);
    }
}