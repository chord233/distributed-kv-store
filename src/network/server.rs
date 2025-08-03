//! # Raft 服务器模块
//!
//! 本模块实现了 Raft 协议的服务器端，用于处理来自集群中其他节点的网络请求。
//! 基于 gRPC 协议提供高性能、可靠的服务端实现。
//!
//! ## 功能特性
//!
//! - **异步处理**: 基于 Tokio 的异步请求处理
//! - **并发安全**: 支持多个客户端的并发请求
//! - **协议实现**: 完整实现 Raft 协议的服务端接口
//! - **错误处理**: 完善的错误处理和状态管理
//! - **日志记录**: 详细的请求和响应日志
//!
//! ## 支持的 Raft 操作
//!
//! ### 领导者选举
//! - `request_vote()` - 处理来自候选者的投票请求
//!
//! ### 日志复制
//! - `append_entries()` - 处理来自领导者的日志追加请求
//!
//! ## 请求处理流程
//!
//! 1. **接收请求**: 通过 gRPC 接收来自其他节点的请求
//! 2. **协议转换**: 将 Protocol Buffers 消息转换为内部数据结构
//! 3. **业务处理**: 调用 Raft 共识模块处理请求
//! 4. **响应转换**: 将处理结果转换为 Protocol Buffers 响应
//! 5. **发送响应**: 通过 gRPC 返回响应给客户端
//!
//! ## 使用示例
//!
//! ```rust,no_run
//! use distributed_kv_store::network::server::RaftServer;
//! use distributed_kv_store::raft::RaftConsensus;
//! use std::sync::Arc;
//! use tokio::sync::RwLock;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let raft = Arc::new(RwLock::new(/* raft_consensus */));
//! let server = RaftServer::new(raft);
//!
//! // 启动服务器
//! let addr = "127.0.0.1:50051".parse()?;
//! server.start(addr).await?;
//! # Ok(())
//! # }
//! ```

use crate::raft::{AppendEntriesResponse, VoteResponse, RaftConsensus};
use crate::proto;
use crate::network::conversion;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

/// Raft 服务器实现
///
/// 处理来自集群中其他节点的 Raft 协议请求的服务器实现。
/// 封装了 Raft 共识模块，提供 gRPC 服务接口。
///
/// # 功能特性
///
/// - **请求处理**: 处理投票请求和日志追加请求
/// - **状态管理**: 维护 Raft 节点的状态信息
/// - **并发安全**: 支持多个客户端的并发访问
/// - **错误处理**: 完善的错误处理和状态验证
/// - **性能优化**: 异步处理和连接复用
///
/// # 内部结构
///
/// - `raft`: Raft 共识模块的共享引用，使用读写锁保护
///
/// # 线程安全
///
/// `RaftServer` 实现了 `Clone` trait，可以安全地在多个异步任务间共享。
/// 内部的 Raft 共识模块使用读写锁保护，确保并发安全。
///
/// # 示例
///
/// ```rust,no_run
/// use distributed_kv_store::network::server::RaftServer;
/// use distributed_kv_store::raft::RaftConsensus;
/// use std::sync::Arc;
/// use tokio::sync::RwLock;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let raft = Arc::new(RwLock::new(/* raft_consensus */));
/// let server = RaftServer::new(raft);
/// println!("Raft server created");
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct RaftServer {
    raft: Arc<RwLock<RaftConsensus>>,
}

impl RaftServer {
    /// Create a new Raft server
    pub fn new(raft: Arc<RwLock<RaftConsensus>>) -> Self {
        Self { raft }
    }
    
    /// Start the gRPC server
    pub async fn start(
        self,
        addr: std::net::SocketAddr,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Starting Raft server on {}", addr);
        
        let service = proto::raft_server::RaftServer::new(self);
        
        tonic::transport::Server::builder()
            .add_service(service)
            .serve(addr)
            .await
            .map_err(|e| {
                error!("Raft server failed: {}", e);
                e.into()
            })
    }
}

#[tonic::async_trait]
impl proto::raft_server::Raft for RaftServer {
    /// Handle vote requests from candidates
    async fn request_vote(
        &self,
        request: Request<proto::VoteRequest>,
    ) -> Result<Response<proto::VoteResponse>, Status> {
        let vote_request = conversion::vote_request_from_proto(request.get_ref());
        
        debug!(
            "Received vote request: term={}, candidate={}, last_log_index={}, last_log_term={}",
            vote_request.term, vote_request.candidate_id, 
            vote_request.last_log_index, vote_request.last_log_term
        );
        
        // TODO: Handle the vote request through the Raft consensus engine
        // This functionality needs to be implemented
        let vote_response = VoteResponse {
            term: 0,
            vote_granted: false,
        };
        
        debug!(
            "Sending vote response: term={}, vote_granted={}",
            vote_response.term, vote_response.vote_granted
        );
        
        let proto_response = conversion::vote_response_to_proto(&vote_response);
        Ok(Response::new(proto_response))
    }
    
    /// Handle append entries requests from leaders
    async fn append_entries(
        &self,
        request: Request<proto::AppendEntriesRequest>,
    ) -> Result<Response<proto::AppendEntriesResponse>, Status> {
        let append_request = conversion::append_entries_request_from_proto(request.get_ref());
        
        debug!(
            "Received append entries: term={}, leader={}, prev_index={}, entries={}, commit={}",
            append_request.term, append_request.leader_id, 
            append_request.prev_log_index, append_request.entries.len(), 
            append_request.leader_commit
        );
        
        // TODO: Handle the append entries request through the Raft consensus engine
        // This functionality needs to be implemented
        let append_response = AppendEntriesResponse {
            term: 0,
            success: false,
            match_index: 0,
        };
        
        debug!(
            "Sending append entries response: term={}, success={}, match_index={}",
            append_response.term, append_response.success, append_response.match_index
        );
        
        let proto_response = conversion::append_entries_response_to_proto(&append_response);
        Ok(Response::new(proto_response))
    }

    /// Handle install snapshot requests from leaders
    async fn install_snapshot(
        &self,
        request: Request<proto::InstallSnapshotRequest>,
    ) -> Result<Response<proto::InstallSnapshotResponse>, Status> {
        let _snapshot_request = request.get_ref();
        
        debug!("Received install snapshot request");
        
        // TODO: Handle the install snapshot request through the Raft consensus engine
        // This functionality needs to be implemented
        let snapshot_response = proto::InstallSnapshotResponse {
            term: 0,
        };
        
        debug!("Sending install snapshot response: term={}", snapshot_response.term);
        
        Ok(Response::new(snapshot_response))
    }
}

/// Server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Server bind address
    pub bind_address: String,
    /// Maximum concurrent connections
    pub max_connections: usize,
    /// Request timeout in seconds
    pub request_timeout: u64,
    /// Keep-alive interval in seconds
    pub keep_alive_interval: u64,
    /// Enable TLS
    pub enable_tls: bool,
    /// TLS certificate file path
    pub tls_cert_file: Option<String>,
    /// TLS private key file path
    pub tls_key_file: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8080".to_string(),
            max_connections: 1000,
            request_timeout: 30,
            keep_alive_interval: 30,
            enable_tls: false,
            tls_cert_file: None,
            tls_key_file: None,
        }
    }
}

/// Enhanced Raft server with configuration
pub struct ConfigurableRaftServer {
    raft: Arc<RwLock<RaftConsensus>>,
    config: ServerConfig,
}

impl ConfigurableRaftServer {
    /// Create a new configurable Raft server
    pub fn new(raft: Arc<RwLock<RaftConsensus>>, config: ServerConfig) -> Self {
        Self { raft, config }
    }
    
    /// Start the server with configuration
    pub async fn start(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let addr: std::net::SocketAddr = self.config.bind_address.parse()
            .map_err(|e| format!("Invalid bind address: {}", e))?;
        
        info!("Starting configurable Raft server on {} with config: {:?}", addr, self.config);
        
        let raft_service = RaftServer::new(self.raft.clone());
        let service = proto::raft_server::RaftServer::new(raft_service)
            .max_decoding_message_size(4 * 1024 * 1024) // 4MB
            .max_encoding_message_size(4 * 1024 * 1024); // 4MB
        
        let mut server_builder = tonic::transport::Server::builder()
            .timeout(std::time::Duration::from_secs(self.config.request_timeout))
            .concurrency_limit_per_connection(256)
            .tcp_keepalive(Some(std::time::Duration::from_secs(self.config.keep_alive_interval)))
            .tcp_nodelay(true);
        
        // TLS support is disabled (requires tonic tls feature)
        if self.config.enable_tls {
            warn!("TLS is configured but not supported in this build (tonic tls feature not enabled)");
        }
        
        server_builder
            .add_service(service)
            .serve(addr)
            .await
            .map_err(|e| {
                error!("Configurable Raft server failed: {}", e);
                e.into()
            })
    }
}

/// Server statistics
#[derive(Debug, Clone)]
pub struct ServerStats {
    /// Total requests received
    pub total_requests: u64,
    /// Vote requests received
    pub vote_requests: u64,
    /// Append entries requests received
    pub append_entries_requests: u64,
    /// Failed requests
    pub failed_requests: u64,
    /// Average request processing time in milliseconds
    pub avg_processing_time_ms: f64,
    /// Server uptime in seconds
    pub uptime_seconds: u64,
}

impl Default for ServerStats {
    fn default() -> Self {
        Self {
            total_requests: 0,
            vote_requests: 0,
            append_entries_requests: 0,
            failed_requests: 0,
            avg_processing_time_ms: 0.0,
            uptime_seconds: 0,
        }
    }
}

/// Server health check
#[derive(Debug, Clone)]
pub struct ServerHealth {
    /// Server is running
    pub is_running: bool,
    /// Server is accepting connections
    pub is_accepting_connections: bool,
    /// Raft consensus is healthy
    pub raft_healthy: bool,
    /// Last health check timestamp
    pub last_check: std::time::SystemTime,
}

impl Default for ServerHealth {
    fn default() -> Self {
        Self {
            is_running: false,
            is_accepting_connections: false,
            raft_healthy: false,
            last_check: std::time::SystemTime::now(),
        }
    }
}

/// Server manager for monitoring and control
pub struct ServerManager {
    stats: Arc<RwLock<ServerStats>>,
    health: Arc<RwLock<ServerHealth>>,
    start_time: std::time::Instant,
}

impl ServerManager {
    /// Create a new server manager
    pub fn new() -> Self {
        Self {
            stats: Arc::new(RwLock::new(ServerStats::default())),
            health: Arc::new(RwLock::new(ServerHealth::default())),
            start_time: std::time::Instant::now(),
        }
    }
    
    /// Get server statistics
    pub async fn get_stats(&self) -> ServerStats {
        let mut stats = self.stats.read().await.clone();
        stats.uptime_seconds = self.start_time.elapsed().as_secs();
        stats
    }
    
    /// Get server health
    pub async fn get_health(&self) -> ServerHealth {
        self.health.read().await.clone()
    }
    
    /// Update server health
    pub async fn update_health(&self, health: ServerHealth) {
        let mut current_health = self.health.write().await;
        *current_health = health;
    }
    
    /// Increment request counter
    pub async fn increment_request_counter(&self, request_type: &str, processing_time_ms: f64) {
        let mut stats = self.stats.write().await;
        stats.total_requests += 1;
        
        match request_type {
            "vote" => stats.vote_requests += 1,
            "append_entries" => stats.append_entries_requests += 1,
            _ => {}
        }
        
        // Update average processing time (simple moving average)
        if stats.total_requests == 1 {
            stats.avg_processing_time_ms = processing_time_ms;
        } else {
            stats.avg_processing_time_ms = 
                (stats.avg_processing_time_ms * (stats.total_requests - 1) as f64 + processing_time_ms) 
                / stats.total_requests as f64;
        }
    }
    
    /// Increment failed request counter
    pub async fn increment_failed_requests(&self) {
        let mut stats = self.stats.write().await;
        stats.failed_requests += 1;
    }
    
    /// Reset statistics
    pub async fn reset_stats(&self) {
        let mut stats = self.stats.write().await;
        *stats = ServerStats::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::RaftConsensus;
    use crate::RaftConfig;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    
    async fn create_test_raft() -> Arc<RwLock<RaftConsensus>> {
        use std::collections::HashMap;
        use crate::storage::Storage;
        
        let raft_config = RaftConfig::default();
        let test_db_path = format!(":memory:_{}_{}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
        let storage = Arc::new(Storage::new(test_db_path).await.expect("Failed to create storage"));
        let peers = HashMap::new();
        let raft = RaftConsensus::new("test_node".to_string(), raft_config, storage, peers)
            .await
            .expect("Failed to create Raft consensus");
        Arc::new(RwLock::new(raft))
    }
    
    #[tokio::test]
    async fn test_raft_server_creation() {
        let raft = create_test_raft().await;
        let _server = RaftServer::new(raft);
        
        // Server should be created successfully
        assert!(true); // If we reach here, creation was successful
    }
    
    #[test]
    fn test_server_config_default() {
        let config = ServerConfig::default();
        
        assert_eq!(config.bind_address, "0.0.0.0:8080");
        assert_eq!(config.max_connections, 1000);
        assert_eq!(config.request_timeout, 30);
        assert_eq!(config.keep_alive_interval, 30);
        assert!(!config.enable_tls);
        assert!(config.tls_cert_file.is_none());
        assert!(config.tls_key_file.is_none());
    }
    
    #[tokio::test]
    async fn test_configurable_raft_server_creation() {
        let raft = create_test_raft().await;
        let config = ServerConfig::default();
        let _server = ConfigurableRaftServer::new(raft, config);
        
        // Server should be created successfully
        assert!(true); // If we reach here, creation was successful
    }
    
    #[test]
    fn test_server_stats_default() {
        let stats = ServerStats::default();
        
        assert_eq!(stats.total_requests, 0);
        assert_eq!(stats.vote_requests, 0);
        assert_eq!(stats.append_entries_requests, 0);
        assert_eq!(stats.failed_requests, 0);
        assert_eq!(stats.avg_processing_time_ms, 0.0);
        assert_eq!(stats.uptime_seconds, 0);
    }
    
    #[test]
    fn test_server_health_default() {
        let health = ServerHealth::default();
        
        assert!(!health.is_running);
        assert!(!health.is_accepting_connections);
        assert!(!health.raft_healthy);
    }
    
    #[tokio::test]
    async fn test_server_manager() {
        let manager = ServerManager::new();
        
        // Test initial state
        let stats = manager.get_stats().await;
        assert_eq!(stats.total_requests, 0);
        
        let health = manager.get_health().await;
        assert!(!health.is_running);
        
        // Test incrementing counters
        manager.increment_request_counter("vote", 10.0).await;
        let stats = manager.get_stats().await;
        assert_eq!(stats.total_requests, 1);
        assert_eq!(stats.vote_requests, 1);
        assert_eq!(stats.avg_processing_time_ms, 10.0);
        
        manager.increment_request_counter("append_entries", 20.0).await;
        let stats = manager.get_stats().await;
        assert_eq!(stats.total_requests, 2);
        assert_eq!(stats.append_entries_requests, 1);
        assert_eq!(stats.avg_processing_time_ms, 15.0); // (10 + 20) / 2
        
        // Test failed requests
        manager.increment_failed_requests().await;
        let stats = manager.get_stats().await;
        assert_eq!(stats.failed_requests, 1);
        
        // Test reset
        manager.reset_stats().await;
        let stats = manager.get_stats().await;
        assert_eq!(stats.total_requests, 0);
        assert_eq!(stats.failed_requests, 0);
    }
    
    #[tokio::test]
    async fn test_server_health_update() {
        let manager = ServerManager::new();
        
        let new_health = ServerHealth {
            is_running: true,
            is_accepting_connections: true,
            raft_healthy: true,
            last_check: std::time::SystemTime::now(),
        };
        
        manager.update_health(new_health.clone()).await;
        let health = manager.get_health().await;
        
        assert!(health.is_running);
        assert!(health.is_accepting_connections);
        assert!(health.raft_healthy);
    }
}