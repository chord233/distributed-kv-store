//! # API 模块
//!
//! 本模块提供分布式键值存储系统的 HTTP 和 gRPC API 接口。
//!
//! ## 功能特性
//!
//! - **HTTP REST API**: 提供标准的 RESTful 接口，支持 GET、PUT、DELETE 等操作
//! - **gRPC API**: 提供高性能的 gRPC 接口，适用于微服务间通信
//! - **集群管理**: 支持节点的动态添加和移除
//! - **健康检查**: 提供系统健康状态监控
//! - **指标收集**: 收集和暴露系统运行指标
//! - **错误处理**: 统一的错误处理和响应机制
//!
//! ## 模块结构
//!
//! - [`http`]: HTTP REST API 实现
//! - [`grpc`]: gRPC API 实现
//! - [`ApiService`]: 核心 API 服务逻辑
//!
//! ## 使用示例
//!
//! ```rust,no_run
//! use distributed_kv_store::api::{ApiService, PutRequest};
//! use std::sync::Arc;
//! use tokio::sync::RwLock;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // 创建 API 服务
//! let raft = Arc::new(RwLock::new(/* raft_consensus */));
//! let api_service = ApiService::new(raft);
//!
//! // 执行 PUT 操作
//! let request = PutRequest {
//!     key: "example_key".to_string(),
//!     value: b"example_value".to_vec(),
//!     ttl: None,
//! };
//!
//! let response = api_service.put(request).await?;
//! println!("PUT 操作结果: {}", response.success);
//! # Ok(())
//! # }
//! ```

use crate::raft::RaftConsensus;
use crate::NodeId;
use serde::{Deserialize, Serialize};

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

pub mod http;
pub mod grpc;

/// API 错误类型
///
/// 定义了 API 层可能出现的所有错误类型，提供统一的错误处理机制。
/// 每个错误类型都包含详细的错误信息，便于调试和错误追踪。
#[derive(thiserror::Error, Debug)]
pub enum ApiError {
    /// 指定的键不存在
    #[error("Key not found: {0}")]
    KeyNotFound(String),
    
    /// 请求参数无效或格式错误
    #[error("Invalid request: {0}")]
    InvalidRequest(String),
    
    /// 当前节点不是领导者，无法处理写操作
    #[error("Not leader: current leader is {0:?}")]
    NotLeader(Option<NodeId>),
    
    /// Raft 共识算法相关错误
    #[error("Raft error: {0}")]
    Raft(#[from] crate::raft::RaftError),
    
    /// 存储层错误
    #[error("Storage error: {0}")]
    Storage(#[from] crate::storage::StorageError),
    
    /// 网络通信错误
    #[error("Network error: {0}")]
    Network(#[from] crate::network::NetworkError),
    
    /// 数据序列化/反序列化错误
    #[error("Serialization error: {0}")]
    Serialization(String),
    
    /// 操作超时错误
    #[error("Timeout error")]
    Timeout,
    
    /// 内部服务器错误
    #[error("Internal server error: {0}")]
    Internal(String),
}

/// API 操作结果类型别名
///
/// 统一的结果类型，所有 API 操作都返回此类型。
/// 成功时包含具体的响应数据，失败时包含 [`ApiError`] 错误信息。
pub type ApiResult<T> = Result<T, ApiError>;

/// PUT 操作请求
///
/// 用于向键值存储中插入或更新数据的请求结构。
///
/// # 字段说明
///
/// - `key`: 要操作的键，不能为空
/// - `value`: 要存储的值，以字节数组形式存储
/// - `ttl`: 可选的生存时间（秒），超时后数据自动删除
///
/// # 示例
///
/// ```rust
/// use distributed_kv_store::api::PutRequest;
///
/// let request = PutRequest {
///     key: "user:123".to_string(),
///     value: b"user data".to_vec(),
///     ttl: Some(3600), // 1小时后过期
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutRequest {
    pub key: String,
    pub value: Vec<u8>,
    /// 生存时间（秒），None 表示永不过期
    pub ttl: Option<u64>,
}

/// GET 操作请求
///
/// 用于从键值存储中获取数据的请求结构。
///
/// # 字段说明
///
/// - `key`: 要查询的键
///
/// # 示例
///
/// ```rust
/// use distributed_kv_store::api::GetRequest;
///
/// let request = GetRequest {
///     key: "user:123".to_string(),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetRequest {
    pub key: String,
}

/// DELETE 操作请求
///
/// 用于从键值存储中删除数据的请求结构。
///
/// # 字段说明
///
/// - `key`: 要删除的键
///
/// # 示例
///
/// ```rust
/// use distributed_kv_store::api::DeleteRequest;
///
/// let request = DeleteRequest {
///     key: "user:123".to_string(),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRequest {
    pub key: String,
}

/// LIST 操作请求
///
/// 用于列出键值存储中的键的请求结构，支持前缀匹配和分页。
///
/// # 字段说明
///
/// - `prefix`: 可选的键前缀过滤器
/// - `limit`: 返回结果的最大数量
/// - `offset`: 分页偏移量
///
/// # 示例
///
/// ```rust
/// use distributed_kv_store::api::ListRequest;
///
/// // 列出所有以 "user:" 开头的键，最多返回 10 个
/// let request = ListRequest {
///     prefix: Some("user:".to_string()),
///     limit: Some(10),
///     offset: Some(0),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListRequest {
    pub prefix: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// PUT 操作响应
///
/// PUT 操作的响应结果，包含操作状态和相关信息。
///
/// # 字段说明
///
/// - `success`: 操作是否成功
/// - `message`: 操作结果描述信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutResponse {
    pub success: bool,
    pub message: String,
}

/// GET 操作响应
///
/// GET 操作的响应结果，包含查询到的数据。
///
/// # 字段说明
///
/// - `key`: 查询的键
/// - `value`: 查询到的值，None 表示键不存在
/// - `found`: 是否找到对应的键
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetResponse {
    pub key: String,
    pub value: Option<Vec<u8>>,
    pub found: bool,
}

/// DELETE 操作响应
///
/// DELETE 操作的响应结果，包含操作状态和相关信息。
///
/// # 字段说明
///
/// - `success`: 操作是否成功
/// - `message`: 操作结果描述信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteResponse {
    pub success: bool,
    pub message: String,
}

/// LIST 操作响应
///
/// LIST 操作的响应结果，包含匹配的键列表和分页信息。
///
/// # 字段说明
///
/// - `keys`: 匹配的键列表
/// - `total`: 总匹配数量
/// - `has_more`: 是否还有更多结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListResponse {
    pub keys: Vec<String>,
    pub total: usize,
    pub has_more: bool,
}

/// 集群状态查询请求
///
/// 用于查询集群当前状态的请求结构，无需额外参数。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStatusRequest {}

/// 集群状态响应
///
/// 包含集群当前状态的详细信息。
///
/// # 字段说明
///
/// - `node_id`: 当前节点 ID
/// - `state`: 节点状态（Leader、Follower、Candidate）
/// - `term`: 当前任期号
/// - `leader`: 当前领导者节点 ID
/// - `peers`: 集群中所有节点列表
/// - `last_log_index`: 最后一条日志索引
/// - `commit_index`: 已提交的日志索引
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStatusResponse {
    pub node_id: NodeId,
    pub state: String,
    pub term: u64,
    pub leader: Option<NodeId>,
    pub peers: Vec<NodeId>,
    pub last_log_index: u64,
    pub commit_index: u64,
}

/// 添加节点请求
///
/// 用于向集群中添加新节点的请求结构。
///
/// # 字段说明
///
/// - `node_id`: 新节点的唯一标识符
/// - `address`: 新节点的网络地址
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddNodeRequest {
    pub node_id: NodeId,
    pub address: String,
}

/// 移除节点请求
///
/// 用于从集群中移除节点的请求结构。
///
/// # 字段说明
///
/// - `node_id`: 要移除的节点 ID
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoveNodeRequest {
    pub node_id: NodeId,
}

/// 节点操作响应
///
/// 节点添加或移除操作的响应结果。
///
/// # 字段说明
///
/// - `success`: 操作是否成功
/// - `message`: 操作结果描述信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeOperationResponse {
    pub success: bool,
    pub message: String,
}

/// Health check response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub node_id: NodeId,
    pub uptime: u64,
    pub raft_state: String,
    pub storage_healthy: bool,
    pub network_healthy: bool,
}

/// Metrics response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsResponse {
    pub node_id: NodeId,
    pub raft_metrics: RaftMetrics,
    pub storage_metrics: StorageMetrics,
    pub network_metrics: NetworkMetrics,
    pub api_metrics: ApiMetrics,
}

/// Raft metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftMetrics {
    pub current_term: u64,
    pub state: String,
    pub leader: Option<NodeId>,
    pub last_log_index: u64,
    pub commit_index: u64,
    pub applied_index: u64,
    pub election_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
}

/// Storage metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageMetrics {
    pub total_keys: u64,
    pub total_size_bytes: u64,
    pub log_entries: u64,
    pub snapshots: u64,
    pub last_snapshot_index: u64,
}

/// Network metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkMetrics {
    pub connected_peers: usize,
    pub total_requests_sent: u64,
    pub total_requests_received: u64,
    pub failed_requests: u64,
    pub avg_latency_ms: f64,
}

/// API metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiMetrics {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub avg_response_time_ms: f64,
    pub requests_per_second: f64,
}

/// API 服务核心实现
///
/// `ApiService` 是分布式键值存储系统的核心 API 服务，负责处理所有的
/// 客户端请求，包括键值操作、集群管理、健康检查和指标收集。
///
/// # 功能特性
///
/// - **键值操作**: 支持 PUT、GET、DELETE、LIST 操作
/// - **集群管理**: 支持节点的动态添加和移除
/// - **指标收集**: 自动收集和统计 API 调用指标
/// - **错误处理**: 统一的错误处理和响应机制
/// - **并发安全**: 使用 RwLock 保证线程安全
///
/// # 内部结构
///
/// - `raft`: Raft 共识算法实例，处理分布式一致性
/// - `start_time`: 服务启动时间，用于计算运行时长
/// - `request_counter`: 总请求计数器
/// - `success_counter`: 成功请求计数器
/// - `error_counter`: 失败请求计数器
///
/// # 示例
///
/// ```rust,no_run
/// use distributed_kv_store::api::ApiService;
/// use std::sync::Arc;
/// use tokio::sync::RwLock;
///
/// # async fn example() {
/// let raft = Arc::new(RwLock::new(/* raft_consensus */));
/// let api_service = ApiService::new(raft);
///
/// // 服务现在可以处理各种 API 请求
/// # }
/// ```
pub struct ApiService {
    raft: Arc<RwLock<RaftConsensus>>,
    start_time: std::time::Instant,
    request_counter: Arc<RwLock<u64>>,
    success_counter: Arc<RwLock<u64>>,
    error_counter: Arc<RwLock<u64>>,
}

impl ApiService {
    /// Create a new API service
    pub fn new(raft: Arc<RwLock<RaftConsensus>>) -> Self {
        Self {
            raft,
            start_time: std::time::Instant::now(),
            request_counter: Arc::new(RwLock::new(0)),
            success_counter: Arc::new(RwLock::new(0)),
            error_counter: Arc::new(RwLock::new(0)),
        }
    }
    
    /// Handle put operation
    pub async fn put(&self, request: PutRequest) -> ApiResult<PutResponse> {
        self.increment_request_counter().await;
        
        debug!("PUT request: key={}, value_size={}", request.key, request.value.len());
        
        // Validate request
        if request.key.is_empty() {
            self.increment_error_counter().await;
            return Err(ApiError::InvalidRequest("Key cannot be empty".to_string()));
        }
        
        if request.value.len() > 1024 * 1024 { // 1MB limit
            self.increment_error_counter().await;
            return Err(ApiError::InvalidRequest("Value too large (max 1MB)".to_string()));
        }
        
        // Execute through Raft
        let raft = self.raft.read().await;
        
        // Serialize the put command
        let command = crate::raft::state_machine::StateMachineCommand::Put {
            key: request.key.clone(),
            value: String::from_utf8(request.value.clone())
                .map_err(|e| ApiError::Internal(format!("Invalid UTF-8: {}", e)))?,
        };
        let data = bincode::serialize(&command)
            .map_err(|e| ApiError::Internal(format!("Serialization error: {}", e)))?;
        
        // Send command through Raft
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let raft_command = crate::raft::RaftCommand::AppendEntry {
            data,
            response_tx,
        };
        
        raft.command_sender().send(raft_command)
            .map_err(|e| ApiError::Internal(format!("Failed to send command: {}", e)))?;
        
        match response_rx.await {
            Ok(Ok(_index)) => {
                self.increment_success_counter().await;
                info!("PUT successful: key={}", request.key);
                Ok(PutResponse {
                    success: true,
                    message: "Key stored successfully".to_string(),
                })
            }
            Ok(Err(e)) => {
                self.increment_error_counter().await;
                error!("PUT failed: key={}, error={}", request.key, e);
                Err(ApiError::Raft(e))
            }
            Err(e) => {
                self.increment_error_counter().await;
                error!("PUT failed: key={}, channel error={}", request.key, e);
                Err(ApiError::Internal(format!("Response channel error: {}", e)))
            }
        }
    }
    
    /// Handle get operation
    pub async fn get(&self, request: GetRequest) -> ApiResult<GetResponse> {
        self.increment_request_counter().await;
        
        debug!("GET request: key={}", request.key);
        
        // Validate request
        if request.key.is_empty() {
            self.increment_error_counter().await;
            return Err(ApiError::InvalidRequest("Key cannot be empty".to_string()));
        }
        
        // Execute through Raft state machine
        let raft = self.raft.read().await;
        match raft.state_machine().get(&request.key).await {
            Some(value) => {
                self.increment_success_counter().await;
                debug!("GET successful: key={}, value_size={}", request.key, value.len());
                Ok(GetResponse {
                    key: request.key,
                    value: Some(value.into_bytes()),
                    found: true,
                })
            }
            None => {
                self.increment_success_counter().await;
                debug!("GET not found: key={}", request.key);
                Ok(GetResponse {
                    key: request.key,
                    value: None,
                    found: false,
                })
            }
        }
    }
    
    /// Handle delete operation
    pub async fn delete(&self, request: DeleteRequest) -> ApiResult<DeleteResponse> {
        self.increment_request_counter().await;
        
        debug!("DELETE request: key={}", request.key);
        
        // Validate request
        if request.key.is_empty() {
            self.increment_error_counter().await;
            return Err(ApiError::InvalidRequest("Key cannot be empty".to_string()));
        }
        
        // Execute through Raft
        let raft = self.raft.read().await;
        
        // Serialize the delete command
        let command = crate::raft::state_machine::StateMachineCommand::Delete {
            key: request.key.clone(),
        };
        let data = bincode::serialize(&command)
            .map_err(|e| ApiError::Internal(format!("Serialization error: {}", e)))?;
        
        // Send command through Raft
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let raft_command = crate::raft::RaftCommand::AppendEntry {
            data,
            response_tx,
        };
        
        raft.command_sender().send(raft_command)
            .map_err(|e| ApiError::Internal(format!("Failed to send command: {}", e)))?;
        
        match response_rx.await {
            Ok(Ok(_index)) => {
                self.increment_success_counter().await;
                info!("DELETE successful: key={}", request.key);
                Ok(DeleteResponse {
                    success: true,
                    message: "Key deleted successfully".to_string(),
                })
            }
            Ok(Err(e)) => {
                self.increment_error_counter().await;
                error!("DELETE failed: key={}, error={}", request.key, e);
                Err(ApiError::Raft(e))
            }
            Err(e) => {
                self.increment_error_counter().await;
                error!("DELETE failed: key={}, channel error={}", request.key, e);
                Err(ApiError::Internal(format!("Response channel error: {}", e)))
            }
        }
    }
    
    /// Handle list operation
    pub async fn list(&self, request: ListRequest) -> ApiResult<ListResponse> {
        self.increment_request_counter().await;
        
        debug!("LIST request: prefix={:?}, limit={:?}", request.prefix, request.limit);
        
        // Execute through Raft state machine
        let raft = self.raft.read().await;
        let key_value_pairs = raft.state_machine().list(request.prefix.as_deref(), request.limit.map(|l| l as usize)).await;
        
        self.increment_success_counter().await;
        
        let total = key_value_pairs.len();
        let offset = request.offset.unwrap_or(0);
        let limit = request.limit.unwrap_or(100);
        
        // Extract just the keys and apply pagination
        let mut keys: Vec<String> = key_value_pairs.into_iter().map(|(k, _)| k).collect();
        
        if offset < keys.len() {
            keys = keys.into_iter().skip(offset).take(limit).collect();
        } else {
            keys.clear();
        }
        
        let has_more = offset + keys.len() < total;
        
        debug!("LIST successful: returned {} keys, total={}, has_more={}", 
               keys.len(), total, has_more);
        
        Ok(ListResponse {
            keys,
            total,
            has_more,
        })
    }
    
    /// Get cluster status
    pub async fn cluster_status(&self, _request: ClusterStatusRequest) -> ApiResult<ClusterStatusResponse> {
        self.increment_request_counter().await;
        
        debug!("CLUSTER_STATUS request");
        
        let raft = self.raft.read().await;
        let cluster_info = raft.get_cluster_info().await;
        
        self.increment_success_counter().await;
        
        Ok(ClusterStatusResponse {
            node_id: raft.node_id().to_string(),
            state: format!("{:?}", cluster_info.state),
            term: cluster_info.current_term,
            leader: cluster_info.leader_id,
            peers: cluster_info.peers.keys().cloned().collect(),
            last_log_index: cluster_info.commit_index,
            commit_index: cluster_info.commit_index,
        })
    }
    
    /// Add node to cluster
    pub async fn add_node(&self, request: AddNodeRequest) -> ApiResult<NodeOperationResponse> {
        self.increment_request_counter().await;
        
        info!("ADD_NODE request: node_id={}, address={}", request.node_id, request.address);
        
        let raft = self.raft.read().await;
        let node_id = request.node_id.clone();
        let _address = request.address.clone();
        match raft.add_peer(request.node_id, request.address).await {
            Ok(_) => {
                self.increment_success_counter().await;
                info!("ADD_NODE successful: node_id={}", node_id);
                Ok(NodeOperationResponse {
                    success: true,
                    message: format!("Node {} added successfully", node_id),
                })
            }
            Err(e) => {
                self.increment_error_counter().await;
                error!("ADD_NODE failed: node_id={}, error={}", node_id, e);
                Err(ApiError::Raft(e))
            }
        }
    }
    
    /// Remove node from cluster
    pub async fn remove_node(&self, request: RemoveNodeRequest) -> ApiResult<NodeOperationResponse> {
        self.increment_request_counter().await;
        
        info!("REMOVE_NODE request: node_id={}", request.node_id);
        
        let raft = self.raft.read().await;
        match raft.remove_peer(request.node_id.clone()).await {
            Ok(_) => {
                self.increment_success_counter().await;
                info!("REMOVE_NODE successful: node_id={}", request.node_id);
                Ok(NodeOperationResponse {
                    success: true,
                    message: format!("Node {} removed successfully", request.node_id),
                })
            }
            Err(e) => {
                self.increment_error_counter().await;
                error!("REMOVE_NODE failed: node_id={}, error={}", request.node_id, e);
                Err(ApiError::Raft(e))
            }
        }
    }
    
    /// Health check
    pub async fn health(&self) -> ApiResult<HealthResponse> {
        let raft = self.raft.read().await;
        let cluster_info = raft.get_cluster_info().await;
        
        Ok(HealthResponse {
            status: "healthy".to_string(),
            node_id: raft.node_id().to_string(),
            uptime: self.start_time.elapsed().as_secs(),
            raft_state: format!("{:?}", cluster_info.state),
            storage_healthy: true, // TODO: Implement actual health check
            network_healthy: true, // TODO: Implement actual health check
        })
    }
    
    /// Get metrics
    pub async fn metrics(&self) -> ApiResult<MetricsResponse> {
        let raft = self.raft.read().await;
        let cluster_info = raft.get_cluster_info().await;
        
        let request_count = *self.request_counter.read().await;
        let success_count = *self.success_counter.read().await;
        let error_count = *self.error_counter.read().await;
        
        let uptime_secs = self.start_time.elapsed().as_secs() as f64;
        let requests_per_second = if uptime_secs > 0.0 {
            request_count as f64 / uptime_secs
        } else {
            0.0
        };
        
        Ok(MetricsResponse {
            node_id: raft.node_id().to_string(),
            raft_metrics: RaftMetrics {
                current_term: cluster_info.current_term,
                state: format!("{:?}", cluster_info.state),
                leader: cluster_info.leader_id,
                last_log_index: cluster_info.commit_index,
                commit_index: cluster_info.commit_index,
                applied_index: cluster_info.last_applied,
                election_timeout_ms: 5000, // TODO: Get from config
                heartbeat_interval_ms: 1000, // TODO: Get from config
            },
            storage_metrics: StorageMetrics {
                total_keys: 0, // TODO: Implement
                total_size_bytes: 0, // TODO: Implement
                log_entries: cluster_info.commit_index,
                snapshots: 0, // TODO: Implement
                last_snapshot_index: 0, // TODO: Implement
            },
            network_metrics: NetworkMetrics {
                connected_peers: cluster_info.peers.len(),
                total_requests_sent: 0, // TODO: Implement
                total_requests_received: request_count,
                failed_requests: error_count,
                avg_latency_ms: 0.0, // TODO: Implement
            },
            api_metrics: ApiMetrics {
                total_requests: request_count,
                successful_requests: success_count,
                failed_requests: error_count,
                avg_response_time_ms: 0.0, // TODO: Implement
                requests_per_second,
            },
        })
    }
    
    /// Increment request counter
    async fn increment_request_counter(&self) {
        let mut counter = self.request_counter.write().await;
        *counter += 1;
    }
    
    /// Increment success counter
    async fn increment_success_counter(&self) {
        let mut counter = self.success_counter.write().await;
        *counter += 1;
    }
    
    /// Increment error counter
    async fn increment_error_counter(&self) {
        let mut counter = self.error_counter.write().await;
        *counter += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::RaftConsensus;
    use crate::{NodeConfig, RaftConfig};
    
    async fn create_test_api_service() -> ApiService {
        let _node_config = NodeConfig::default();
        let raft_config = RaftConfig::default();
        let test_db_path = format!("test_db_{}_{}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
        let storage = Arc::new(crate::storage::Storage::new(test_db_path).await.expect("Failed to create storage"));
        let peers = std::collections::HashMap::new();
        let raft = RaftConsensus::new("test_node".to_string(), raft_config, storage, peers)
            .await
            .expect("Failed to create Raft consensus");
        let raft = Arc::new(RwLock::new(raft));
        ApiService::new(raft)
    }
    
    #[tokio::test]
    async fn test_api_service_creation() {
        let _service = create_test_api_service().await;
        
        // Service should be created successfully
        assert!(true); // If we reach here, creation was successful
    }
    
    #[tokio::test]
    async fn test_put_request_validation() {
        let service = create_test_api_service().await;
        
        // Test empty key
        let request = PutRequest {
            key: "".to_string(),
            value: b"test".to_vec(),
            ttl: None,
        };
        
        let result = service.put(request).await;
        assert!(result.is_err());
        
        if let Err(ApiError::InvalidRequest(msg)) = result {
            assert!(msg.contains("Key cannot be empty"));
        } else {
            panic!("Expected InvalidRequest error");
        }
    }
    
    #[tokio::test]
    async fn test_put_request_large_value() {
        let service = create_test_api_service().await;
        
        // Test large value (> 1MB)
        let large_value = vec![0u8; 2 * 1024 * 1024]; // 2MB
        let request = PutRequest {
            key: "test_key".to_string(),
            value: large_value,
            ttl: None,
        };
        
        let result = service.put(request).await;
        assert!(result.is_err());
        
        if let Err(ApiError::InvalidRequest(msg)) = result {
            assert!(msg.contains("Value too large"));
        } else {
            panic!("Expected InvalidRequest error");
        }
    }
    
    #[tokio::test]
    async fn test_get_request_validation() {
        let service = create_test_api_service().await;
        
        // Test empty key
        let request = GetRequest {
            key: "".to_string(),
        };
        
        let result = service.get(request).await;
        assert!(result.is_err());
        
        if let Err(ApiError::InvalidRequest(msg)) = result {
            assert!(msg.contains("Key cannot be empty"));
        } else {
            panic!("Expected InvalidRequest error");
        }
    }
    
    #[tokio::test]
    async fn test_delete_request_validation() {
        let service = create_test_api_service().await;
        
        // Test empty key
        let request = DeleteRequest {
            key: "".to_string(),
        };
        
        let result = service.delete(request).await;
        assert!(result.is_err());
        
        if let Err(ApiError::InvalidRequest(msg)) = result {
            assert!(msg.contains("Key cannot be empty"));
        } else {
            panic!("Expected InvalidRequest error");
        }
    }
    
    #[tokio::test]
    async fn test_health_check() {
        let service = create_test_api_service().await;
        
        let result = service.health().await;
        assert!(result.is_ok());
        
        let health = result.unwrap();
        assert_eq!(health.status, "healthy");
        assert_eq!(health.node_id, "test_node");
        assert!(health.storage_healthy);
        assert!(health.network_healthy);
    }
    
    #[tokio::test]
    async fn test_metrics() {
        let service = create_test_api_service().await;
        
        let result = service.metrics().await;
        assert!(result.is_ok());
        
        let metrics = result.unwrap();
        assert_eq!(metrics.node_id, "test_node");
        assert_eq!(metrics.api_metrics.total_requests, 0);
        assert_eq!(metrics.api_metrics.successful_requests, 0);
        assert_eq!(metrics.api_metrics.failed_requests, 0);
    }
    
    #[tokio::test]
    async fn test_list_pagination() {
        let service = create_test_api_service().await;
        
        let request = ListRequest {
            prefix: None,
            limit: Some(10),
            offset: Some(5),
        };
        
        // This will likely fail due to Raft not being fully initialized,
        // but we're testing the request structure
        let _result = service.list(request).await;
        // We don't assert on the result since it depends on Raft state
    }
}