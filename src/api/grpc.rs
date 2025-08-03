//! # gRPC API 服务器实现
//!
//! 本模块实现了分布式键值存储系统的 gRPC API 服务器，基于 Tonic 框架构建。
//! 提供了完整的 gRPC 服务端和客户端实现，支持以下功能：
//! 
//! ## 主要功能
//! - 键值对的基本操作（PUT、GET、DELETE、LIST）
//! - 集群节点管理（添加节点、移除节点、获取集群信息）
//! - TLS 加密通信支持
//! - gRPC 反射服务支持
//! - 可配置的消息大小限制
//! 
//! ## 架构设计
//! - `GrpcServer`: gRPC 服务器实现，处理客户端请求
//! - `GrpcClient`: gRPC 客户端实现，用于与服务器通信
//! - `GrpcConfig`: 服务器配置结构体
//! - 错误转换函数，将内部 API 错误转换为 gRPC Status

use crate::api::{
    ApiService, PutRequest, GetRequest, DeleteRequest, ListRequest,
    AddNodeRequest, RemoveNodeRequest,
    ApiError,
};
use crate::proto;
use std::sync::Arc;
use tonic::{Request, Response, Status, transport::Server};
use tracing::{debug, error, info};

/// gRPC 服务器配置
/// 
/// 包含 gRPC 服务器运行所需的所有配置参数，支持序列化和反序列化
/// 以便从配置文件中加载设置。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GrpcConfig {
    /// 服务器绑定地址，格式为 "IP:端口"
    pub bind_address: String,
    /// 最大消息大小限制（字节），防止过大的消息占用过多内存
    pub max_message_size: usize,
    /// 是否启用 TLS 加密通信
    pub enable_tls: bool,
    /// TLS 证书文件路径（启用 TLS 时必需）
    pub tls_cert_file: Option<String>,
    /// TLS 私钥文件路径（启用 TLS 时必需）
    pub tls_key_file: Option<String>,
    /// 是否启用 gRPC 反射服务，用于调试和工具集成
    pub enable_reflection: bool,
}

impl Default for GrpcConfig {
    /// 创建默认的 gRPC 配置
    /// 
    /// 默认配置：
    /// - 绑定地址：127.0.0.1:8080
    /// - 最大消息大小：4MB
    /// - 禁用 TLS
    /// - 禁用反射服务
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1:8080".to_string(),
            max_message_size: 4 * 1024 * 1024, // 4MB
            enable_tls: false,
            tls_cert_file: None,
            tls_key_file: None,
            enable_reflection: false,
        }
    }
}

/// gRPC 服务器实现
/// 
/// 封装了 API 服务层，实现 gRPC 协议的键值存储服务。
/// 使用 Arc 智能指针共享 API 服务实例，支持多线程并发访问。
pub struct GrpcServer {
    /// API 服务层的共享引用，处理实际的业务逻辑
    api_service: Arc<ApiService>,
}

impl GrpcServer {
    /// 创建新的 gRPC 服务器实例
    /// 
    /// # 参数
    /// * `api_service` - API 服务层的共享引用
    /// 
    /// # 返回值
    /// 返回新创建的 gRPC 服务器实例
    pub fn new(api_service: Arc<ApiService>) -> Self {
        Self { api_service }
    }

    /// 启动 gRPC 服务器
    /// 
    /// 根据提供的配置启动 gRPC 服务器，支持 TLS 加密和反射服务。
    /// 此方法会阻塞当前线程直到服务器关闭。
    /// 
    /// # 参数
    /// * `config` - gRPC 服务器配置
    /// 
    /// # 返回值
    /// * `Ok(())` - 服务器正常关闭
    /// * `Err(...)` - 启动或运行过程中发生错误
    /// 
    /// # 错误
    /// - 地址解析失败
    /// - TLS 配置错误
    /// - 证书文件读取失败
    /// - 服务器绑定失败
    pub async fn start(self, config: GrpcConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let addr = config.bind_address.parse()?;
        info!("Starting gRPC server on {}", addr);

        let kv_service = proto::kv_store_server::KvStoreServer::new(self)
            .max_decoding_message_size(config.max_message_size)
            .max_encoding_message_size(config.max_message_size);

        let mut server_builder = Server::builder();

        // TLS 配置 - 如果启用了 TLS，则配置证书和私钥
        if config.enable_tls {
            if let (Some(cert_file), Some(key_file)) = (config.tls_cert_file, config.tls_key_file) {
                let cert = tokio::fs::read(cert_file).await
                    .map_err(|e| format!("Failed to read certificate file: {}", e))?;
                let key = tokio::fs::read(key_file).await
                    .map_err(|e| format!("Failed to read private key file: {}", e))?;
                
                let identity = tonic::transport::Identity::from_pem(cert, key);
                let tls_config = tonic::transport::ServerTlsConfig::new().identity(identity);
                
                server_builder = server_builder.tls_config(tls_config)
                    .map_err(|e| format!("Failed to configure TLS: {}", e))?;
                
                info!("TLS enabled for gRPC server");
            } else {
                return Err("TLS enabled but certificate or key file not specified".into());
            }
        }

        let mut service_builder = server_builder.add_service(kv_service);

        // 反射服务 - 启用 gRPC 反射，允许客户端动态发现服务定义
        if config.enable_reflection {
            let reflection_service = tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
                .build()
                .map_err(|e| format!("Failed to build reflection service: {}", e))?;
            
            service_builder = service_builder.add_service(reflection_service);
            info!("gRPC reflection enabled");
        }

        info!("gRPC server listening on {}", addr);
        service_builder.serve(addr).await
            .map_err(|e| {
                error!("gRPC server failed: {}", e);
                e.into()
            })
    }
}

/// 实现 gRPC 服务接口
/// 
/// 为 GrpcServer 实现 proto 定义的 KvStore 服务接口，
/// 处理所有 gRPC 请求并将其转换为内部 API 调用。
#[tonic::async_trait]
impl proto::kv_store_server::KvStore for GrpcServer {
    /// 存储键值对
    /// 
    /// 处理 gRPC PUT 请求，将键值对存储到分布式存储系统中。
    /// 
    /// # 参数
    /// * `request` - 包含键和值的 gRPC 请求
    /// 
    /// # 返回值
    /// * `Ok(Response<PutResponse>)` - 操作成功，返回响应
    /// * `Err(Status)` - 操作失败，返回 gRPC 错误状态
    async fn put(
        &self,
        request: Request<proto::PutRequest>,
    ) -> Result<Response<proto::PutResponse>, Status> {
        let req = request.into_inner();
        debug!("gRPC PUT: key={}, value_size={}", req.key, req.value.len());
        
        let put_request = PutRequest {
            key: req.key,
            value: req.value.into(),
            ttl: None,
        };
        
        match self.api_service.put(put_request).await {
            Ok(response) => {
                Ok(Response::new(proto::PutResponse {
                    success: response.success,
                    error: if response.success { String::new() } else { "Put operation failed".to_string() },
                }))
            }
            Err(e) => Err(convert_api_error(e)),
        }
    }
    
    /// 根据键获取值
    /// 
    /// 处理 gRPC GET 请求，从分布式存储系统中检索指定键的值。
    /// 
    /// # 参数
    /// * `request` - 包含要查询的键的 gRPC 请求
    /// 
    /// # 返回值
    /// * `Ok(Response<GetResponse>)` - 查询成功，返回值和查找状态
    /// * `Err(Status)` - 查询失败，返回 gRPC 错误状态
    async fn get(
        &self,
        request: Request<proto::GetRequest>,
    ) -> Result<Response<proto::GetResponse>, Status> {
        let req = request.into_inner();
        debug!("gRPC GET: key={}", req.key);
        
        let get_request = GetRequest {
            key: req.key,
        };
        
        match self.api_service.get(get_request).await {
            Ok(response) => {
                Ok(Response::new(proto::GetResponse {
                    found: response.found,
                    value: String::from_utf8(response.value.unwrap_or_default()).unwrap_or_default(),
                    error: String::new(),
                }))
            }
            Err(e) => Err(convert_api_error(e)),
        }
    }
    
    /// 删除指定键
    /// 
    /// 处理 gRPC DELETE 请求，从分布式存储系统中删除指定的键值对。
    /// 
    /// # 参数
    /// * `request` - 包含要删除的键的 gRPC 请求
    /// 
    /// # 返回值
    /// * `Ok(Response<DeleteResponse>)` - 删除操作完成，返回操作状态
    /// * `Err(Status)` - 删除失败，返回 gRPC 错误状态
    async fn delete(
        &self,
        request: Request<proto::DeleteRequest>,
    ) -> Result<Response<proto::DeleteResponse>, Status> {
        let req = request.into_inner();
        debug!("gRPC DELETE: key={}", req.key);
        
        let delete_request = DeleteRequest {
            key: req.key,
        };
        
        match self.api_service.delete(delete_request).await {
            Ok(response) => {
                Ok(Response::new(proto::DeleteResponse {
                    success: response.success,
                    error: if response.success { String::new() } else { response.message },
                }))
            }
            Err(e) => {
                Ok(Response::new(proto::DeleteResponse {
                    success: false,
                    error: e.to_string(),
                }))
            }
        }
    }
    
    /// 列出键（支持前缀过滤）
    /// 
    /// 处理 gRPC LIST 请求，返回存储系统中的键列表。
    /// 支持按前缀过滤和限制返回数量。
    /// 
    /// # 参数
    /// * `request` - 包含前缀和限制条件的 gRPC 请求
    /// 
    /// # 返回值
    /// * `Ok(Response<ListResponse>)` - 列出成功，返回键值对列表
    /// * `Err(Status)` - 列出失败，返回 gRPC 错误状态
    async fn list(
        &self,
        request: Request<proto::ListRequest>,
    ) -> Result<Response<proto::ListResponse>, Status> {
        let req = request.into_inner();
        debug!("gRPC LIST: prefix={:?}, limit={}", req.prefix, req.limit);
        
        let list_request = ListRequest {
            prefix: if req.prefix.is_empty() { None } else { Some(req.prefix) },
            limit: if req.limit > 0 { Some(req.limit as usize) } else { None },
            offset: None,
        };
        
        match self.api_service.list(list_request).await {
            Ok(response) => {
                // 将键列表转换为 KeyValue 对象
                // 注意：List 操作只返回键名，值字段为空以节省网络带宽
                let pairs = response.keys.into_iter().map(|key| proto::KeyValue {
                    key,
                    value: String::new(), // List 操作只返回键，不返回值
                }).collect();
                Ok(Response::new(proto::ListResponse {
                    pairs,
                    error: String::new(),
                }))
            }
            Err(e) => {
                Ok(Response::new(proto::ListResponse {
                    pairs: vec![],
                    error: e.to_string(),
                }))
            }
        }
    }
    
    /// 向集群添加节点
    /// 
    /// 处理 gRPC ADD_NODE 请求，将新节点添加到分布式集群中。
    /// 只有集群领导者可以执行此操作。
    /// 
    /// # 参数
    /// * `request` - 包含节点 ID 和地址的 gRPC 请求
    /// 
    /// # 返回值
    /// * `Ok(Response<AddNodeResponse>)` - 添加成功，返回操作状态
    /// * `Err(Status)` - 添加失败，返回 gRPC 错误状态
    async fn add_node(
        &self,
        request: Request<proto::AddNodeRequest>,
    ) -> Result<Response<proto::AddNodeResponse>, Status> {
        let req = request.into_inner();
        debug!("gRPC ADD_NODE: node_id={}, address={}", req.node_id, req.address);
        
        let add_node_request = AddNodeRequest {
            node_id: req.node_id,
            address: req.address,
        };
        
        match self.api_service.add_node(add_node_request).await {
            Ok(response) => {
                Ok(Response::new(proto::AddNodeResponse {
                    success: response.success,
                    error: if response.success { String::new() } else { response.message },
                }))
            }
            Err(e) => Err(convert_api_error(e)),
        }
    }
    
    /// 从集群移除节点
    /// 
    /// 处理 gRPC REMOVE_NODE 请求，从分布式集群中移除指定节点。
    /// 只有集群领导者可以执行此操作。
    /// 
    /// # 参数
    /// * `request` - 包含要移除的节点 ID 的 gRPC 请求
    /// 
    /// # 返回值
    /// * `Ok(Response<RemoveNodeResponse>)` - 移除成功，返回操作状态
    /// * `Err(Status)` - 移除失败，返回 gRPC 错误状态
    async fn remove_node(
        &self,
        request: Request<proto::RemoveNodeRequest>,
    ) -> Result<Response<proto::RemoveNodeResponse>, Status> {
        let req = request.into_inner();
        debug!("gRPC REMOVE_NODE: node_id={}", req.node_id);
        
        let remove_node_request = RemoveNodeRequest {
            node_id: req.node_id,
        };
        
        match self.api_service.remove_node(remove_node_request).await {
            Ok(response) => {
                Ok(Response::new(proto::RemoveNodeResponse {
                    success: response.success,
                    error: if response.success { String::new() } else { response.message },
                }))
            }
            Err(e) => Err(convert_api_error(e)),
        }
    }
    
    /// 获取集群信息
    /// 
    /// 处理 gRPC GET_CLUSTER_INFO 请求，返回当前集群的状态信息，
    /// 包括所有节点、当前领导者和 Raft 任期号。
    /// 
    /// # 参数
    /// * `_request` - gRPC 请求（无需参数）
    /// 
    /// # 返回值
    /// * `Ok(Response<GetClusterInfoResponse>)` - 获取成功，返回集群信息
    /// * `Err(Status)` - 获取失败，返回 gRPC 错误状态
    async fn get_cluster_info(
        &self,
        _request: Request<proto::GetClusterInfoRequest>,
    ) -> Result<Response<proto::GetClusterInfoResponse>, Status> {
        debug!("gRPC GET_CLUSTER_INFO");
        
        let status_request = crate::api::ClusterStatusRequest {};
        match self.api_service.cluster_status(status_request).await {
            Ok(status) => {
                // 将内部节点信息转换为 gRPC 响应格式
                let nodes = status.peers.into_iter().map(|peer| proto::NodeInfo {
                    node_id: peer,
                    address: String::new(), // TODO: 需要从集群配置中获取实际地址
                    state: status.state.clone(),
                    last_heartbeat: 0, // TODO: 需要实现心跳时间戳跟踪
                }).collect();
                
                Ok(Response::new(proto::GetClusterInfoResponse {
                    nodes,
                    leader_id: status.leader.unwrap_or_default(),
                    term: status.term as i64,
                }))
            }
            Err(e) => Err(convert_api_error(e)),
        }
    }
}

/// gRPC 客户端
/// 
/// 提供与 gRPC 服务器通信的客户端接口，封装了所有的 gRPC 调用。
/// 使用 Tonic 框架的客户端实现，支持连接池和自动重连。
pub struct GrpcClient {
    /// Tonic gRPC 客户端实例
    client: proto::kv_store_client::KvStoreClient<tonic::transport::Channel>,
    /// 服务器地址，用于标识连接目标
    address: String,
}

impl GrpcClient {
    /// 连接到 gRPC 服务器
    /// 
    /// 建立与指定地址的 gRPC 服务器的连接。
    /// 
    /// # 参数
    /// * `address` - 服务器地址，格式为 "http://IP:端口" 或 "https://IP:端口"
    /// 
    /// # 返回值
    /// * `Ok(GrpcClient)` - 连接成功，返回客户端实例
    /// * `Err(...)` - 连接失败，返回错误信息
    /// 
    /// # 错误
    /// - 网络连接失败
    /// - 地址格式错误
    /// - 服务器不可达
    pub async fn connect(address: String) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let client = proto::kv_store_client::KvStoreClient::connect(address.clone()).await?;
        Ok(Self { client, address })
    }
    
    /// 存储键值对
    /// 
    /// 向服务器发送 PUT 请求，存储指定的键值对。
    /// 
    /// # 参数
    /// * `key` - 要存储的键
    /// * `value` - 要存储的值
    /// 
    /// # 返回值
    /// * `Ok(PutResponse)` - 存储成功，返回服务器响应
    /// * `Err(Status)` - 存储失败，返回 gRPC 错误状态
    pub async fn put(&mut self, key: String, value: String) -> Result<proto::PutResponse, Status> {
        let request = proto::PutRequest { key, value };
        let response = self.client.put(Request::new(request)).await?;
        Ok(response.into_inner())
    }
    
    /// 根据键获取值
    /// 
    /// 向服务器发送 GET 请求，获取指定键的值。
    /// 
    /// # 参数
    /// * `key` - 要查询的键
    /// 
    /// # 返回值
    /// * `Ok(GetResponse)` - 查询完成，返回值和查找状态
    /// * `Err(Status)` - 查询失败，返回 gRPC 错误状态
    pub async fn get(&mut self, key: String) -> Result<proto::GetResponse, Status> {
        let request = proto::GetRequest { key };
        let response = self.client.get(Request::new(request)).await?;
        Ok(response.into_inner())
    }
    
    /// 删除指定键
    /// 
    /// 向服务器发送 DELETE 请求，删除指定的键值对。
    /// 
    /// # 参数
    /// * `key` - 要删除的键
    /// 
    /// # 返回值
    /// * `Ok(DeleteResponse)` - 删除完成，返回操作状态
    /// * `Err(Status)` - 删除失败，返回 gRPC 错误状态
    pub async fn delete(&mut self, key: String) -> Result<proto::DeleteResponse, Status> {
        let request = proto::DeleteRequest { key };
        let response = self.client.delete(Request::new(request)).await?;
        Ok(response.into_inner())
    }
    
    /// 列出键
    /// 
    /// 向服务器发送 LIST 请求，获取键列表。
    /// 
    /// # 参数
    /// * `prefix` - 可选的键前缀过滤条件
    /// * `limit` - 可选的返回数量限制
    /// 
    /// # 返回值
    /// * `Ok(ListResponse)` - 列出成功，返回键值对列表
    /// * `Err(Status)` - 列出失败，返回 gRPC 错误状态
    pub async fn list(
        &mut self,
        prefix: Option<String>,
        limit: Option<i32>,
    ) -> Result<proto::ListResponse, Status> {
        let request = proto::ListRequest {
            prefix: prefix.unwrap_or_default(),
            limit: limit.unwrap_or(0),
        };
        let response = self.client.list(Request::new(request)).await?;
        Ok(response.into_inner())
    }
    
    /// 获取集群信息
    /// 
    /// 向服务器发送 GET_CLUSTER_INFO 请求，获取集群状态信息。
    /// 
    /// # 返回值
    /// * `Ok(GetClusterInfoResponse)` - 获取成功，返回集群信息
    /// * `Err(Status)` - 获取失败，返回 gRPC 错误状态
    pub async fn get_cluster_info(&mut self) -> Result<proto::GetClusterInfoResponse, Status> {
        let request = proto::GetClusterInfoRequest {};
        let response = self.client.get_cluster_info(Request::new(request)).await?;
        Ok(response.into_inner())
    }
    
    /// 向集群添加节点
    /// 
    /// 向服务器发送 ADD_NODE 请求，将新节点添加到集群中。
    /// 
    /// # 参数
    /// * `node_id` - 新节点的唯一标识符
    /// * `address` - 新节点的网络地址
    /// 
    /// # 返回值
    /// * `Ok(AddNodeResponse)` - 添加完成，返回操作状态
    /// * `Err(Status)` - 添加失败，返回 gRPC 错误状态
    pub async fn add_node(&mut self, node_id: String, address: String) -> Result<proto::AddNodeResponse, Status> {
        let request = proto::AddNodeRequest { node_id, address };
        let response = self.client.add_node(Request::new(request)).await?;
        Ok(response.into_inner())
    }
    
    /// 从集群移除节点
    /// 
    /// 向服务器发送 REMOVE_NODE 请求，从集群中移除指定节点。
    /// 
    /// # 参数
    /// * `node_id` - 要移除的节点标识符
    /// 
    /// # 返回值
    /// * `Ok(RemoveNodeResponse)` - 移除完成，返回操作状态
    /// * `Err(Status)` - 移除失败，返回 gRPC 错误状态
    pub async fn remove_node(&mut self, node_id: String) -> Result<proto::RemoveNodeResponse, Status> {
        let request = proto::RemoveNodeRequest { node_id };
        let response = self.client.remove_node(Request::new(request)).await?;
        Ok(response.into_inner())
    }
    
    /// 获取客户端连接的服务器地址
    /// 
    /// # 返回值
    /// 返回当前连接的服务器地址字符串引用
    pub fn address(&self) -> &str {
        &self.address
    }
}

/// 将 API 错误转换为 gRPC Status
/// 
/// 将内部 API 层的错误类型转换为 gRPC 协议的 Status 错误，
/// 确保错误信息能够正确传递给 gRPC 客户端。
/// 
/// # 参数
/// * `error` - 内部 API 错误
/// 
/// # 返回值
/// 对应的 gRPC Status 错误
fn convert_api_error(error: ApiError) -> Status {
    match error {
        // 键未找到 -> NOT_FOUND
        ApiError::KeyNotFound(key) => Status::not_found(format!("Key not found: {}", key)),
        // 无效请求 -> INVALID_ARGUMENT
        ApiError::InvalidRequest(msg) => Status::invalid_argument(format!("Invalid request: {}", msg)),
        // 非领导者节点 -> UNAVAILABLE（客户端应重试其他节点）
        ApiError::NotLeader(leader) => {
            let msg = match leader {
                Some(leader_id) => format!("Not leader, current leader: {}", leader_id),
                None => "Not leader, no current leader".to_string(),
            };
            Status::unavailable(msg)
        }
        // Raft 共识错误 -> INTERNAL
        ApiError::Raft(e) => Status::internal(format!("Raft error: {}", e)),
        // 存储层错误 -> INTERNAL
        ApiError::Storage(e) => Status::internal(format!("Storage error: {}", e)),
        // 网络错误 -> UNAVAILABLE
        ApiError::Network(e) => Status::unavailable(format!("Network error: {}", e)),
        // 序列化错误 -> INTERNAL
        ApiError::Serialization(msg) => Status::internal(format!("Serialization error: {}", msg)),
        // 超时错误 -> DEADLINE_EXCEEDED
        ApiError::Timeout => Status::deadline_exceeded("Request timeout"),
        // 内部错误 -> INTERNAL
        ApiError::Internal(msg) => Status::internal(format!("Internal error: {}", msg)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    
    /// 测试 gRPC 配置的默认值
    #[tokio::test]
    async fn test_grpc_config_default() {
        let config = GrpcConfig::default();
        // 验证默认绑定地址
        assert_eq!(config.bind_address, "127.0.0.1:8080");
        // 验证默认消息大小限制（4MB）
        assert_eq!(config.max_message_size, 4 * 1024 * 1024);
        // 验证默认禁用 TLS
        assert!(!config.enable_tls);
        // 验证默认禁用反射服务
        assert!(!config.enable_reflection);
    }
}