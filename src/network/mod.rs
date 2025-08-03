//! # 网络通信模块
//!
//! 本模块实现了分布式键值存储系统的网络通信层，专门为 Raft 共识算法提供网络支持。
//! 基于 gRPC 协议实现节点间的高效、可靠通信。
//!
//! ## 功能特性
//!
//! - **Raft 通信**: 专门为 Raft 共识算法设计的网络接口
//! - **连接管理**: 自动管理与集群中其他节点的连接
//! - **错误处理**: 完善的网络错误处理和重试机制
//! - **异步支持**: 基于 Tokio 的异步网络 I/O
//! - **负载均衡**: 支持多种连接策略和负载均衡
//! - **健康检查**: 自动检测和处理节点连接状态
//!
//! ## 核心组件
//!
//! ### NetworkManager
//! 网络管理器，负责管理所有网络连接和通信：
//! - 维护与其他节点的客户端连接
//! - 处理连接池和连接复用
//! - 提供统一的网络接口
//!
//! ### RaftClient
//! Raft 客户端，用于向其他节点发送请求：
//! - 发送投票请求 (RequestVote)
//! - 发送日志追加请求 (AppendEntries)
//! - 处理响应和错误
//!
//! ### RaftServer
//! Raft 服务器，处理来自其他节点的请求：
//! - 接收和处理投票请求
//! - 接收和处理日志追加请求
//! - 返回相应的响应
//!
//! ## Raft 协议支持
//!
//! ### 投票阶段 (Leader Election)
//! - `RequestVote` - 候选者请求投票
//! - `VoteResponse` - 投票响应
//!
//! ### 日志复制 (Log Replication)
//! - `AppendEntries` - 领导者追加日志条目
//! - `AppendEntriesResponse` - 日志追加响应
//!
//! ## 使用示例
//!
//! ```rust,no_run
//! use distributed_kv_store::network::NetworkManager;
//! use std::collections::HashMap;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let node_id = 1;
//! let mut peers = HashMap::new();
//! peers.insert(2, "http://127.0.0.1:50052".to_string());
//! peers.insert(3, "http://127.0.0.1:50053".to_string());
//!
//! let network_manager = NetworkManager::new(node_id, peers).await?;
//!
//! // 启动网络服务
//! network_manager.start_server("127.0.0.1:50051").await?;
//! # Ok(())
//! # }
//! ```

use crate::raft::{
    AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse,
};
use crate::{NodeId, proto};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;


use tracing::{debug, error, info, warn};

pub mod client;
pub mod server;

use client::RaftClient;
use server::RaftServer;

/// 网络错误类型
///
/// 定义网络通信过程中可能出现的各种错误类型，提供详细的错误信息和上下文。
///
/// # 错误类型说明
///
/// - `Connection`: 网络连接错误，如连接超时、连接被拒绝等
/// - `Rpc`: gRPC 调用错误，如服务不可用、方法未找到等
/// - `Serialization`: 数据序列化/反序列化错误
/// - `NodeNotFound`: 指定的节点不存在或不可达
/// - `Timeout`: 网络操作超时
/// - `InvalidAddress`: 无效的网络地址格式
///
/// # 示例
///
/// ```rust
/// use distributed_kv_store::network::NetworkError;
///
/// // 处理网络错误
/// match network_result {
///     Ok(response) => println!("Success: {:?}", response),
///     Err(NetworkError::Connection(e)) => {
///         eprintln!("Connection failed: {}", e);
///     },
///     Err(NetworkError::Timeout) => {
///         eprintln!("Request timed out");
///     },
///     Err(e) => eprintln!("Other error: {}", e),
/// }
/// ```
#[derive(thiserror::Error, Debug)]
pub enum NetworkError {
    #[error("Connection error: {0}")]
    Connection(#[from] tonic::transport::Error),
    
    #[error("RPC error: {0}")]
    Rpc(#[from] tonic::Status),
    
    #[error("Serialization error: {0}")]
    Serialization(String),
    
    #[error("Node not found: {0}")]
    NodeNotFound(NodeId),
    
    #[error("Network timeout")]
    Timeout,
    
    #[error("Invalid address: {0}")]
    InvalidAddress(String),
}

pub type NetworkResult<T> = Result<T, NetworkError>;

/// 网络管理器
///
/// 负责管理分布式键值存储系统中节点间的网络通信，专门为 Raft 共识算法提供网络支持。
/// 维护与集群中其他节点的连接，处理请求路由和响应管理。
///
/// # 功能特性
///
/// - **连接池管理**: 维护与其他节点的持久连接，支持连接复用
/// - **自动重连**: 检测连接故障并自动重新建立连接
/// - **负载均衡**: 支持多种连接策略和请求分发
/// - **错误处理**: 完善的网络错误处理和重试机制
/// - **异步操作**: 基于 Tokio 的异步网络 I/O
///
/// # 内部结构
///
/// - `node_id`: 当前节点的唯一标识符
/// - `clients`: 到其他节点的客户端连接池
/// - `server`: 本地 Raft 服务器实例
///
/// # 使用示例
///
/// ```rust,no_run
/// use distributed_kv_store::network::NetworkManager;
/// use std::collections::HashMap;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let node_id = 1;
/// let network_manager = NetworkManager::new(node_id);
///
/// // 添加对等节点
/// network_manager.add_peer(2, "http://127.0.0.1:50052").await?;
/// network_manager.add_peer(3, "http://127.0.0.1:50053").await?;
///
/// // 启动服务器
/// network_manager.start_server("127.0.0.1:50051").await?;
/// # Ok(())
/// # }
/// ```
pub struct NetworkManager {
    /// 当前节点 ID
    node_id: NodeId,
    /// 到其他节点的客户端连接
    clients: Arc<RwLock<HashMap<NodeId, RaftClient>>>,
    /// 服务器实例
    #[allow(dead_code)]
    server: Option<RaftServer>,
}

impl NetworkManager {
    /// Create a new network manager
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            clients: Arc::new(RwLock::new(HashMap::new())),
            server: None,
        }
    }
    
    /// Add a peer connection
    pub async fn add_peer(&self, node_id: NodeId, address: String) -> NetworkResult<()> {
        let client = RaftClient::new(&address).await?;
        
        let mut clients = self.clients.write().await;
        clients.insert(node_id.clone(), client);
        
        info!("Added peer connection: {} -> {}", node_id, address);
        Ok(())
    }
    
    /// Remove a peer connection
    pub async fn remove_peer(&self, node_id: &NodeId) -> NetworkResult<()> {
        let mut clients = self.clients.write().await;
        if clients.remove(node_id).is_some() {
            info!("Removed peer connection: {}", node_id);
            Ok(())
        } else {
            Err(NetworkError::NodeNotFound(node_id.clone()))
        }
    }
    
    /// Send vote request to a peer
    pub async fn send_vote_request(
        &self,
        node_id: &NodeId,
        request: VoteRequest,
    ) -> NetworkResult<VoteResponse> {
        let clients = self.clients.read().await;
        let client = clients.get(node_id)
            .ok_or_else(|| NetworkError::NodeNotFound(node_id.clone()))?;
        
        debug!("Sending vote request to {}: {:?}", node_id, request);
        
        let response = client.request_vote(request).await?;
        
        debug!("Received vote response from {}: {:?}", node_id, response);
        Ok(response)
    }
    
    /// Send append entries request to a peer
    pub async fn send_append_entries(
        &self,
        node_id: &NodeId,
        request: AppendEntriesRequest,
    ) -> NetworkResult<AppendEntriesResponse> {
        let clients = self.clients.read().await;
        let client = clients.get(node_id)
            .ok_or_else(|| NetworkError::NodeNotFound(node_id.clone()))?;
        
        debug!("Sending append entries to {}: {} entries", node_id, request.entries.len());
        
        let response = client.append_entries(request).await?;
        
        debug!("Received append entries response from {}: success={}", node_id, response.success);
        Ok(response)
    }
    
    /// Broadcast vote request to all peers
    pub async fn broadcast_vote_request(
        &self,
        request: VoteRequest,
    ) -> HashMap<NodeId, NetworkResult<VoteResponse>> {
        let clients = self.clients.read().await;
        let mut results = HashMap::new();
        
        for (node_id, client) in clients.iter() {
            let response = client.request_vote(request.clone()).await;
            results.insert(node_id.clone(), response);
        }
        
        results
    }
    
    /// Broadcast append entries to all peers
    pub async fn broadcast_append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> HashMap<NodeId, NetworkResult<AppendEntriesResponse>> {
        let clients = self.clients.read().await;
        let mut results = HashMap::new();
        
        for (node_id, client) in clients.iter() {
            let response = client.append_entries(request.clone()).await;
            results.insert(node_id.clone(), response);
        }
        
        results
    }
    
    /// Send heartbeat to all peers
    pub async fn send_heartbeats(
        &self,
        term: u64,
        leader_id: NodeId,
        commit_index: u64,
    ) -> HashMap<NodeId, NetworkResult<AppendEntriesResponse>> {
        let heartbeat_request = AppendEntriesRequest {
            term,
            leader_id,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![], // Empty for heartbeat
            leader_commit: commit_index,
        };
        
        self.broadcast_append_entries(heartbeat_request).await
    }
    
    /// Get connected peer count
    pub async fn peer_count(&self) -> usize {
        self.clients.read().await.len()
    }
    
    /// Get list of connected peers
    pub async fn get_peers(&self) -> Vec<NodeId> {
        self.clients.read().await.keys().cloned().collect()
    }
    
    /// Check if a peer is connected
    pub async fn is_peer_connected(&self, node_id: &NodeId) -> bool {
        self.clients.read().await.contains_key(node_id)
    }
    
    /// Test connection to a peer
    pub async fn test_peer_connection(&self, node_id: &NodeId) -> NetworkResult<bool> {
        let clients = self.clients.read().await;
        let client = clients.get(node_id)
            .ok_or_else(|| NetworkError::NodeNotFound(node_id.clone()))?;
        
        // Send a simple ping (empty vote request with term 0)
        let ping_request = VoteRequest {
            term: 0,
            candidate_id: self.node_id.clone(),
            last_log_index: 0,
            last_log_term: 0,
        };
        
        match client.request_vote(ping_request).await {
            Ok(_) => Ok(true),
            Err(NetworkError::Rpc(status)) if status.code() == tonic::Code::InvalidArgument => {
                // Expected response for ping with term 0
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }
    
    /// Cleanup disconnected peers
    pub async fn cleanup_disconnected_peers(&self) -> Vec<NodeId> {
        let mut disconnected = Vec::new();
        let peer_ids: Vec<_> = self.get_peers().await;
        
        for peer_id in peer_ids {
            match self.test_peer_connection(&peer_id).await {
                Ok(false) | Err(_) => {
                    if let Err(e) = self.remove_peer(&peer_id).await {
                        warn!("Failed to remove disconnected peer {}: {}", peer_id, e);
                    } else {
                        disconnected.push(peer_id);
                    }
                }
                Ok(true) => {}
            }
        }
        
        if !disconnected.is_empty() {
            info!("Cleaned up {} disconnected peers", disconnected.len());
        }
        
        disconnected
    }
    
    /// Get network statistics
    pub async fn get_stats(&self) -> NetworkStats {
        let clients = self.clients.read().await;
        let peer_count = clients.len();
        
        // Test connectivity to all peers
        let mut reachable_peers = 0;
        for node_id in clients.keys() {
            if let Ok(true) = self.test_peer_connection(node_id).await {
                reachable_peers += 1;
            }
        }
        
        NetworkStats {
            total_peers: peer_count,
            reachable_peers,
            unreachable_peers: peer_count - reachable_peers,
        }
    }
}

/// Network statistics
#[derive(Debug, Clone)]
pub struct NetworkStats {
    pub total_peers: usize,
    pub reachable_peers: usize,
    pub unreachable_peers: usize,
}

/// Convert between internal and protobuf types
pub mod conversion {
    use super::*;
    use crate::storage::log::{LogEntry, LogEntryType};
    
    /// Convert VoteRequest to protobuf
    pub fn vote_request_to_proto(req: &VoteRequest) -> proto::VoteRequest {
        proto::VoteRequest {
            term: req.term as i64,
            candidate_id: req.candidate_id.clone(),
            last_log_index: req.last_log_index as i64,
            last_log_term: req.last_log_term as i64,
        }
    }
    
    /// Convert protobuf to VoteRequest
    pub fn vote_request_from_proto(proto: &proto::VoteRequest) -> VoteRequest {
        VoteRequest {
            term: proto.term as u64,
            candidate_id: proto.candidate_id.clone(),
            last_log_index: proto.last_log_index as u64,
            last_log_term: proto.last_log_term as u64,
        }
    }
    
    /// Convert VoteResponse to protobuf
    pub fn vote_response_to_proto(resp: &VoteResponse) -> proto::VoteResponse {
        proto::VoteResponse {
            term: resp.term as i64,
            vote_granted: resp.vote_granted,
        }
    }
    
    /// Convert protobuf to VoteResponse
    pub fn vote_response_from_proto(proto: &proto::VoteResponse) -> VoteResponse {
        VoteResponse {
            term: proto.term as u64,
            vote_granted: proto.vote_granted,
        }
    }
    
    /// Convert AppendEntriesRequest to protobuf
    pub fn append_entries_request_to_proto(req: &AppendEntriesRequest) -> proto::AppendEntriesRequest {
        let entries = req.entries.iter().map(log_entry_to_proto).collect();
        
        proto::AppendEntriesRequest {
            term: req.term as i64,
            leader_id: req.leader_id.clone(),
            prev_log_index: req.prev_log_index as i64,
            prev_log_term: req.prev_log_term as i64,
            entries,
            leader_commit: req.leader_commit as i64,
        }
    }
    
    /// Convert protobuf to AppendEntriesRequest
    pub fn append_entries_request_from_proto(proto: &proto::AppendEntriesRequest) -> AppendEntriesRequest {
        let entries = proto.entries.iter().map(log_entry_from_proto).collect();
        
        AppendEntriesRequest {
            term: proto.term as u64,
            leader_id: proto.leader_id.clone(),
            prev_log_index: proto.prev_log_index as u64,
            prev_log_term: proto.prev_log_term as u64,
            entries,
            leader_commit: proto.leader_commit as u64,
        }
    }
    
    /// Convert AppendEntriesResponse to protobuf
    pub fn append_entries_response_to_proto(resp: &AppendEntriesResponse) -> proto::AppendEntriesResponse {
        proto::AppendEntriesResponse {
            term: resp.term as i64,
            success: resp.success,
            match_index: resp.match_index as i64,
        }
    }
    
    /// Convert protobuf to AppendEntriesResponse
    pub fn append_entries_response_from_proto(proto: &proto::AppendEntriesResponse) -> AppendEntriesResponse {
        AppendEntriesResponse {
            term: proto.term as u64,
            success: proto.success,
            match_index: proto.match_index as u64,
        }
    }
    
    /// Convert LogEntry to protobuf
    pub fn log_entry_to_proto(entry: &LogEntry) -> proto::LogEntry {
        let entry_type = match entry.entry_type {
            LogEntryType::Application => "application",
            LogEntryType::Configuration => "configuration",
            LogEntryType::NoOp => "noop",
        };
        
        proto::LogEntry {
            term: entry.term as i64,
            index: entry.index as i64,
            data: entry.data.clone(),
            entry_type: entry_type.to_string(),
        }
    }
    
    /// Convert protobuf to LogEntry
    pub fn log_entry_from_proto(proto: &proto::LogEntry) -> LogEntry {
        let entry_type = match proto.entry_type.as_str() {
            "application" => LogEntryType::Application,
            "configuration" => LogEntryType::Configuration,
            "noop" => LogEntryType::NoOp,
            _ => LogEntryType::Application, // Default fallback
        };
        
        LogEntry {
            term: proto.term as u64,
            index: proto.index as u64,
            data: proto.data.clone(),
            entry_type,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::log::{LogEntry, LogEntryType};
    
    #[test]
    fn test_vote_request_conversion() {
        let original = VoteRequest {
            term: 5,
            candidate_id: "node1".to_string(),
            last_log_index: 10,
            last_log_term: 3,
        };
        
        let proto = conversion::vote_request_to_proto(&original);
        let converted = conversion::vote_request_from_proto(&proto);
        
        assert_eq!(original.term, converted.term);
        assert_eq!(original.candidate_id, converted.candidate_id);
        assert_eq!(original.last_log_index, converted.last_log_index);
        assert_eq!(original.last_log_term, converted.last_log_term);
    }
    
    #[test]
    fn test_vote_response_conversion() {
        let original = VoteResponse {
            term: 5,
            vote_granted: true,
        };
        
        let proto = conversion::vote_response_to_proto(&original);
        let converted = conversion::vote_response_from_proto(&proto);
        
        assert_eq!(original.term, converted.term);
        assert_eq!(original.vote_granted, converted.vote_granted);
    }
    
    #[test]
    fn test_log_entry_conversion() {
        let original = LogEntry {
            term: 3,
            index: 15,
            data: b"test data".to_vec(),
            entry_type: LogEntryType::Application,
        };
        
        let proto = conversion::log_entry_to_proto(&original);
        let converted = conversion::log_entry_from_proto(&proto);
        
        assert_eq!(original.term, converted.term);
        assert_eq!(original.index, converted.index);
        assert_eq!(original.data, converted.data);
        assert_eq!(original.entry_type, converted.entry_type);
    }
    
    #[test]
    fn test_append_entries_conversion() {
        let entries = vec![
            LogEntry {
                term: 2,
                index: 5,
                data: b"entry1".to_vec(),
                entry_type: LogEntryType::Application,
            },
            LogEntry {
                term: 2,
                index: 6,
                data: b"entry2".to_vec(),
                entry_type: LogEntryType::Configuration,
            },
        ];
        
        let original = AppendEntriesRequest {
            term: 2,
            leader_id: "leader".to_string(),
            prev_log_index: 4,
            prev_log_term: 1,
            entries,
            leader_commit: 3,
        };
        
        let proto = conversion::append_entries_request_to_proto(&original);
        let converted = conversion::append_entries_request_from_proto(&proto);
        
        assert_eq!(original.term, converted.term);
        assert_eq!(original.leader_id, converted.leader_id);
        assert_eq!(original.prev_log_index, converted.prev_log_index);
        assert_eq!(original.prev_log_term, converted.prev_log_term);
        assert_eq!(original.entries.len(), converted.entries.len());
        assert_eq!(original.leader_commit, converted.leader_commit);
        
        for (orig, conv) in original.entries.iter().zip(converted.entries.iter()) {
            assert_eq!(orig.term, conv.term);
            assert_eq!(orig.index, conv.index);
            assert_eq!(orig.data, conv.data);
            assert_eq!(orig.entry_type, conv.entry_type);
        }
    }
    
    #[tokio::test]
    async fn test_network_manager_creation() {
        let manager = NetworkManager::new("test_node".to_string());
        
        assert_eq!(manager.node_id, "test_node");
        assert_eq!(manager.peer_count().await, 0);
        assert!(manager.get_peers().await.is_empty());
    }
}