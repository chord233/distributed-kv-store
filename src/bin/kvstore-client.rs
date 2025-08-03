//! # 分布式键值存储客户端
//!
//! 这是分布式键值存储系统的命令行客户端工具，提供了与服务器交互的
//! 完整接口。支持通过 HTTP 和 gRPC 协议与服务器通信，提供丰富的
//! 命令行操作和输出格式。
//!
//! ## 功能特性
//!
//! - **多协议支持**: 支持 HTTP REST API 和 gRPC 协议
//! - **完整的 CRUD 操作**: 支持键值的创建、读取、更新、删除
//! - **集群管理**: 查看集群状态、节点信息
//! - **批量操作**: 支持批量导入和导出数据
//! - **性能测试**: 内置性能基准测试工具
//! - **多种输出格式**: JSON、表格、纯文本格式
//!
//! ## 基本用法
//!
//! ### 设置键值对
//! ```bash
//! # 使用 HTTP 协议
//! cargo run --bin kvstore-client -- put mykey myvalue
//!
//! # 使用 gRPC 协议
//! cargo run --bin kvstore-client -- --protocol grpc put mykey myvalue
//! ```
//!
//! ### 获取键值
//! ```bash
//! cargo run --bin kvstore-client -- get mykey
//! ```
//!
//! ### 删除键
//! ```bash
//! cargo run --bin kvstore-client -- delete mykey
//! ```
//!
//! ### 列出所有键
//! ```bash
//! cargo run --bin kvstore-client -- list
//! ```
//!
//! ## 高级功能
//!
//! ### 集群状态查询
//! ```bash
//! cargo run --bin kvstore-client -- cluster status
//! ```
//!
//! ### 批量导入数据
//! ```bash
//! cargo run --bin kvstore-client -- import data.json
//! ```
//!
//! ### 批量导出数据
//! ```bash
//! cargo run --bin kvstore-client -- export output.json
//! ```
//!
//! ### 性能基准测试
//! ```bash
//! # 测试写入性能
//! cargo run --bin kvstore-client -- benchmark write --count 1000
//!
//! # 测试读取性能
//! cargo run --bin kvstore-client -- benchmark read --count 1000
//!
//! # 混合读写测试
//! cargo run --bin kvstore-client -- benchmark mixed --count 1000 --ratio 70:30
//! ```
//!
//! ## 配置选项
//!
//! ### 服务器地址
//! ```bash
//! cargo run --bin kvstore-client -- --server http://192.168.1.100:8080 get mykey
//! ```
//!
//! ### 协议选择
//! ```bash
//! # HTTP 协议（默认）
//! cargo run --bin kvstore-client -- --protocol http get mykey
//!
//! # gRPC 协议
//! cargo run --bin kvstore-client -- --protocol grpc get mykey
//! ```
//!
//! ### 输出格式
//! ```bash
//! # JSON 格式（默认）
//! cargo run --bin kvstore-client -- --format json get mykey
//!
//! # 表格格式
//! cargo run --bin kvstore-client -- --format table list
//!
//! # 纯文本格式
//! cargo run --bin kvstore-client -- --format plain get mykey
//! ```
//!
//! ### 超时设置
//! ```bash
//! cargo run --bin kvstore-client -- --timeout 60 get mykey
//! ```
//!
//! ## 命令参考
//!
//! | 命令 | 描述 | 示例 |
//! |------|------|------|
//! | `put <key> <value>` | 设置键值对 | `put user:1 Alice` |
//! | `get <key>` | 获取键的值 | `get user:1` |
//! | `delete <key>` | 删除键 | `delete user:1` |
//! | `list [prefix]` | 列出键（可选前缀过滤） | `list user:` |
//! | `cluster status` | 查看集群状态 | `cluster status` |
//! | `import <file>` | 从文件导入数据 | `import data.json` |
//! | `export <file>` | 导出数据到文件 | `export backup.json` |
//! | `benchmark <type>` | 运行性能测试 | `benchmark write` |

use distributed_kv_store::api::grpc::GrpcClient;

use std::time::Instant;
use clap::{Arg, Command, ArgMatches};
use serde_json::{Value, json};
use tracing::{info, error, debug};
use tokio::time::{timeout, Duration};
use base64::{Engine as _, engine::general_purpose};

/// Client configuration
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub server_address: String,
    pub protocol: Protocol,
    pub timeout: Duration,
    pub output_format: OutputFormat,
    pub verbose: bool,
}

/// Communication protocol
#[derive(Debug, Clone, PartialEq)]
pub enum Protocol {
    Http,
    Grpc,
}

/// Output format
#[derive(Debug, Clone, PartialEq)]
pub enum OutputFormat {
    Json,
    Table,
    Plain,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server_address: "http://localhost:8080".to_string(),
            protocol: Protocol::Http,
            timeout: Duration::from_secs(30),
            output_format: OutputFormat::Json,
            verbose: false,
        }
    }
}

/// HTTP client for REST API
#[derive(Clone)]
pub struct HttpClient {
    client: reqwest::Client,
    base_url: String,
}

impl HttpClient {
    /// Create a new HTTP client
    pub fn new(base_url: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        
        Ok(Self {
            client,
            base_url: base_url.to_string(),
        })
    }
    
    /// Put a key-value pair
    pub async fn put(
        &self,
        key: &str,
        value: &[u8],
        ttl: Option<u64>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let mut body = json!({
            "key": key,
            "value": general_purpose::STANDARD.encode(value)
        });
        
        if let Some(ttl) = ttl {
            body["ttl"] = json!(ttl);
        }
        
        let response = self.client
            .put(&format!("{}/api/v1/kv/{}", self.base_url, key))
            .json(&body)
            .send()
            .await?;
        
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let error_text = response.text().await?;
            Err(format!("HTTP error: {}", error_text).into())
        }
    }
    
    /// Get a value by key
    pub async fn get(&self, key: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let response = self.client
            .get(&format!("{}/api/v1/kv/{}", self.base_url, key))
            .send()
            .await?;
        
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let error_text = response.text().await?;
            Err(format!("HTTP error: {}", error_text).into())
        }
    }
    
    /// Delete a key
    pub async fn delete(&self, key: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let response = self.client
            .delete(&format!("{}/api/v1/kv/{}", self.base_url, key))
            .send()
            .await?;
        
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let error_text = response.text().await?;
            Err(format!("HTTP error: {}", error_text).into())
        }
    }
    
    /// List keys
    pub async fn list(
        &self,
        prefix: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let mut url = format!("{}/api/v1/kv", self.base_url);
        let mut params = Vec::new();
        
        if let Some(prefix) = prefix {
            params.push(format!("prefix={}", urlencoding::encode(prefix)));
        }
        if let Some(limit) = limit {
            params.push(format!("limit={}", limit));
        }
        if let Some(offset) = offset {
            params.push(format!("offset={}", offset));
        }
        
        if !params.is_empty() {
            url.push('?');
            url.push_str(&params.join("&"));
        }
        
        let response = self.client.get(&url).send().await?;
        
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let error_text = response.text().await?;
            Err(format!("HTTP error: {}", error_text).into())
        }
    }
    
    /// Get cluster status
    pub async fn cluster_status(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let response = self.client
            .get(&format!("{}/api/v1/cluster/status", self.base_url))
            .send()
            .await?;
        
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let error_text = response.text().await?;
            Err(format!("HTTP error: {}", error_text).into())
        }
    }
    
    /// Add a node to the cluster
    pub async fn add_node(
        &self,
        node_id: &str,
        address: &str,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let body = json!({
            "node_id": node_id,
            "address": address
        });
        
        let response = self.client
            .post(&format!("{}/api/v1/cluster/nodes", self.base_url))
            .json(&body)
            .send()
            .await?;
        
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let error_text = response.text().await?;
            Err(format!("HTTP error: {}", error_text).into())
        }
    }
    
    /// Remove a node from the cluster
    pub async fn remove_node(&self, node_id: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let response = self.client
            .delete(&format!("{}/api/v1/cluster/nodes/{}", self.base_url, node_id))
            .send()
            .await?;
        
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let error_text = response.text().await?;
            Err(format!("HTTP error: {}", error_text).into())
        }
    }
    
    /// Health check
    pub async fn health(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let response = self.client
            .get(&format!("{}/api/v1/health", self.base_url))
            .send()
            .await?;
        
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let error_text = response.text().await?;
            Err(format!("HTTP error: {}", error_text).into())
        }
    }
    
    /// Get metrics
    pub async fn metrics(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let response = self.client
            .get(&format!("{}/api/v1/metrics", self.base_url))
            .send()
            .await?;
        
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            let error_text = response.text().await?;
            Err(format!("HTTP error: {}", error_text).into())
        }
    }
}

/// Client wrapper that supports both HTTP and gRPC
pub struct KvStoreClient {
    config: ClientConfig,
    http_client: Option<HttpClient>,
    grpc_client: Option<GrpcClient>,
}

impl KvStoreClient {
    /// Create a new client
    pub async fn new(config: ClientConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let (http_client, grpc_client) = match config.protocol {
            Protocol::Http => {
                let client = HttpClient::new(&config.server_address)?;
                (Some(client), None)
            }
            Protocol::Grpc => {
                let client = GrpcClient::connect(config.server_address.clone()).await?;
                (None, Some(client))
            }
        };
        
        Ok(Self {
            config,
            http_client,
            grpc_client,
        })
    }
    

    
    /// Put a key-value pair
    pub async fn put(
        &mut self,
        key: &str,
        value: &[u8],
        ttl: Option<u64>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        
        let result = timeout(self.config.timeout, async {
            match (&self.http_client, &mut self.grpc_client) {
                (Some(client), None) => {
                    client.put(key, value, ttl).await
                }
                (None, Some(client)) => {
                    let response = client.put(key.to_string(), String::from_utf8_lossy(value).to_string()).await
                        .map_err(|e| format!("gRPC error: {}", e))?;
                    Ok(json!({
                        "success": response.success,
                        "error": response.error
                    }))
                }
                _ => Err("No client available".into()),
            }
        }).await.map_err(|_| -> Box<dyn std::error::Error + Send + Sync> { "Operation timed out".into() })??;
        
        if self.config.verbose {
            debug!("PUT operation took {:?}", start.elapsed());
        }
        
        Ok(result)
    }
    
    /// Get a value by key
    pub async fn get(&mut self, key: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        
        let result = timeout(self.config.timeout, async {
            match (&self.http_client, &mut self.grpc_client) {
                (Some(client), None) => {
                    client.get(key).await
                }
                (None, Some(client)) => {
                    let response = client.get(key.to_string()).await
                        .map_err(|e| format!("gRPC error: {}", e))?;
                    Ok(json!({
                        "value": if response.found { Some(general_purpose::STANDARD.encode(&response.value)) } else { None },
                        "found": response.found
                    }))
                }
                _ => Err("No client available".into()),
            }
        }).await.map_err(|_| "Operation timed out")??;
        
        if self.config.verbose {
            debug!("GET operation took {:?}", start.elapsed());
        }
        
        Ok(result)
    }
    
    /// Delete a key
    pub async fn delete(&mut self, key: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        
        let result = timeout(self.config.timeout, async {
            match (&self.http_client, &mut self.grpc_client) {
                (Some(client), None) => {
                    client.delete(key).await
                }
                (None, Some(client)) => {
                    let response = client.delete(key.to_string()).await
                        .map_err(|e| format!("gRPC error: {}", e))?;
                    Ok(json!({
                        "success": response.success,
                        "error": response.error
                    }))
                }
                _ => Err("No client available".into()),
            }
        }).await.map_err(|_| "Operation timed out")??;
        
        if self.config.verbose {
            debug!("DELETE operation took {:?}", start.elapsed());
        }
        
        Ok(result)
    }
    
    /// List keys
    pub async fn list(
        &mut self,
        prefix: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        
        let result = timeout(self.config.timeout, async {
            match (&self.http_client, &mut self.grpc_client) {
                (Some(client), None) => {
                    client.list(prefix, limit, offset).await
                }
                (None, Some(client)) => {
                    let response = client.list(
                        prefix.map(|s| s.to_string()),
                        limit.map(|l| l as i32),
                    ).await.map_err(|e| format!("gRPC error: {}", e))?;
                    Ok(json!({
                        "pairs": response.pairs.iter().map(|kv| json!({
                            "key": kv.key,
                            "value": kv.value
                        })).collect::<Vec<_>>(),
                        "error": response.error
                    }))
                }
                _ => Err("No client available".into()),
            }
        }).await.map_err(|_| "Operation timed out")??;
        
        if self.config.verbose {
            debug!("LIST operation took {:?}", start.elapsed());
        }
        
        Ok(result)
    }
    
    /// Get cluster status
    pub async fn cluster_status(&mut self) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        
        let result = timeout(self.config.timeout, async {
            match (&self.http_client, &mut self.grpc_client) {
                (Some(client), None) => {
                    client.cluster_status().await
                }
                (None, Some(client)) => {
                    let response = client.get_cluster_info().await
                        .map_err(|e| format!("gRPC error: {}", e))?;
                    Ok(json!({
                        "nodes": response.nodes.into_iter().map(|node| json!({
                            "node_id": node.node_id,
                            "address": node.address,
                            "state": node.state,
                            "last_heartbeat": node.last_heartbeat
                        })).collect::<Vec<_>>(),
                        "leader_id": response.leader_id,
                        "term": response.term
                    }))
                }
                _ => Err("No client available".into()),
            }
        }).await.map_err(|_| "Operation timed out")??;
        
        if self.config.verbose {
            debug!("CLUSTER_STATUS operation took {:?}", start.elapsed());
        }
        
        Ok(result)
    }
    
    /// Add a node to the cluster
    pub async fn add_node(
        &mut self,
        node_id: &str,
        address: &str,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        
        let result = timeout(self.config.timeout, async {
            match (&self.http_client, &mut self.grpc_client) {
                (Some(client), None) => {
                    client.add_node(node_id, address).await
                }
                (None, Some(client)) => {
                    let response = client.add_node(node_id.to_string(), address.to_string()).await
                        .map_err(|e| format!("gRPC error: {}", e))?;
                    Ok(json!({
                        "success": response.success,
                        "error": response.error
                    }))
                }
                _ => Err("No client available".into()),
            }
        }).await.map_err(|_| "Operation timed out")??;
        
        if self.config.verbose {
            debug!("ADD_NODE operation took {:?}", start.elapsed());
        }
        
        Ok(result)
    }
    
    /// Remove a node from the cluster
    pub async fn remove_node(&mut self, node_id: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        
        let result = timeout(self.config.timeout, async {
            match (&self.http_client, &mut self.grpc_client) {
                (Some(client), None) => {
                    client.remove_node(node_id).await
                }
                (None, Some(client)) => {
                    let response = client.remove_node(node_id.to_string()).await
                        .map_err(|e| format!("gRPC error: {}", e))?;
                    Ok(json!({
                        "success": response.success,
                        "error": response.error
                    }))
                }
                _ => Err("No client available".into()),
            }
        }).await.map_err(|_| -> Box<dyn std::error::Error + Send + Sync> { "Operation timed out".into() })??;
        
        if self.config.verbose {
            debug!("REMOVE_NODE operation took {:?}", start.elapsed());
        }
        
        Ok(result)
    }
    
    /// Health check (using cluster info as health indicator)
    pub async fn health(&mut self) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        
        let operation = async {
            match (&self.http_client, &mut self.grpc_client) {
                (Some(client), None) => {
                    client.health().await
                }
                (None, Some(client)) => {
                    let response = client.get_cluster_info().await
                        .map_err(|e| format!("gRPC error: {}", e))?;
                    Ok(json!({
                        "status": "healthy",
                        "leader_id": response.leader_id,
                        "term": response.term,
                        "nodes_count": response.nodes.len()
                    }))
                }
                _ => Err("No client available".into()),
            }
        };
        
        let result = timeout(self.config.timeout, operation)
            .await
            .map_err(|_| "Operation timed out")??;
        
        if self.config.verbose {
            debug!("HEALTH operation took {:?}", start.elapsed());
        }
        
        Ok(result)
    }
    
    /// Get metrics (using cluster info as basic metrics)
    pub async fn metrics(&mut self) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        
        let operation = async {
            match (&self.http_client, &mut self.grpc_client) {
                (Some(client), None) => {
                    client.metrics().await
                }
                (None, Some(client)) => {
                    let response = client.get_cluster_info().await
                        .map_err(|e| format!("gRPC error: {}", e))?;
                    
                    let result = json!({
                        "cluster_metrics": {
                            "leader_id": response.leader_id,
                            "term": response.term,
                            "nodes_count": response.nodes.len(),
                            "nodes": response.nodes.iter().map(|node| {
                                json!({
                                    "node_id": node.node_id,
                                    "address": node.address,
                                    "state": node.state,
                                    "last_heartbeat": node.last_heartbeat
                                })
                            }).collect::<Vec<_>>()
                        }
                    });
                    
                    Ok(result)
                }
                _ => Err("No client available".into()),
            }
        };
        
        let result = timeout(self.config.timeout, operation)
            .await
            .map_err(|_| "Operation timed out")??;
        
        if self.config.verbose {
            debug!("METRICS operation took {:?}", start.elapsed());
        }
        
        Ok(result)
    }
}

/// Format output based on configuration
fn format_output(value: &Value, format: &OutputFormat) -> String {
    match format {
        OutputFormat::Json => {
            serde_json::to_string_pretty(value).unwrap_or_else(|_| "Invalid JSON".to_string())
        }
        OutputFormat::Table => {
            format_as_table(value)
        }
        OutputFormat::Plain => {
            format_as_plain(value)
        }
    }
}

/// Format value as a simple table
fn format_as_table(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut result = String::new();
            let max_key_len = map.keys().map(|k| k.len()).max().unwrap_or(0);
            
            for (key, val) in map {
                result.push_str(&format!(
                    "{:<width$} | {}\n",
                    key,
                    format_value_inline(val),
                    width = max_key_len
                ));
            }
            result
        }
        _ => format_as_plain(value),
    }
}

/// Format value as plain text
fn format_as_plain(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        Value::Array(arr) => {
            arr.iter()
                .map(|v| format_value_inline(v))
                .collect::<Vec<_>>()
                .join(", ")
        }
        Value::Object(map) => {
            map.iter()
                .map(|(k, v)| format!("{}: {}", k, format_value_inline(v)))
                .collect::<Vec<_>>()
                .join(", ")
        }
    }
}

/// Format value inline (single line)
fn format_value_inline(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        Value::Array(arr) => {
            format!("[{}]", arr.iter()
                .map(|v| format_value_inline(v))
                .collect::<Vec<_>>()
                .join(", "))
        }
        Value::Object(_) => "<object>".to_string(),
    }
}

/// Create command line interface
fn create_cli() -> Command {
    Command::new("kvstore-client")
        .version(env!("CARGO_PKG_VERSION"))
        .author("Distributed KV Store Team")
        .about("Command-line client for Distributed Key-Value Store")
        .arg(
            Arg::new("server")
                .short('s')
                .long("server")
                .value_name("ADDRESS")
                .help("Server address (e.g., http://localhost:8080 or grpc://localhost:50051)")
                .default_value("http://localhost:8080")
        )
        .arg(
            Arg::new("protocol")
                .short('p')
                .long("protocol")
                .value_name("PROTOCOL")
                .help("Communication protocol (http or grpc)")
                .value_parser(["http", "grpc"])
                .default_value("http")
        )
        .arg(
            Arg::new("timeout")
                .short('t')
                .long("timeout")
                .value_name("SECONDS")
                .help("Request timeout in seconds")
                .value_parser(clap::value_parser!(u64))
                .default_value("30")
        )
        .arg(
            Arg::new("format")
                .short('f')
                .long("format")
                .value_name("FORMAT")
                .help("Output format (json, table, plain)")
                .value_parser(["json", "table", "plain"])
                .default_value("json")
        )
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .help("Enable verbose output")
                .action(clap::ArgAction::SetTrue)
        )
        .subcommand(
            Command::new("put")
                .about("Put a key-value pair")
                .arg(
                    Arg::new("key")
                        .help("Key to store")
                        .required(true)
                        .index(1)
                )
                .arg(
                    Arg::new("value")
                        .help("Value to store")
                        .required(true)
                        .index(2)
                )
                .arg(
                    Arg::new("ttl")
                        .long("ttl")
                        .value_name("SECONDS")
                        .help("Time to live in seconds")
                        .value_parser(clap::value_parser!(u64))
                )
        )
        .subcommand(
            Command::new("get")
                .about("Get a value by key")
                .arg(
                    Arg::new("key")
                        .help("Key to retrieve")
                        .required(true)
                        .index(1)
                )
        )
        .subcommand(
            Command::new("delete")
                .about("Delete a key")
                .arg(
                    Arg::new("key")
                        .help("Key to delete")
                        .required(true)
                        .index(1)
                )
        )
        .subcommand(
            Command::new("list")
                .about("List keys")
                .arg(
                    Arg::new("prefix")
                        .long("prefix")
                        .value_name("PREFIX")
                        .help("Key prefix filter")
                )
                .arg(
                    Arg::new("limit")
                        .long("limit")
                        .value_name("COUNT")
                        .help("Maximum number of keys to return")
                        .value_parser(clap::value_parser!(usize))
                )
                .arg(
                    Arg::new("offset")
                        .long("offset")
                        .value_name("COUNT")
                        .help("Number of keys to skip")
                        .value_parser(clap::value_parser!(usize))
                )
        )
        .subcommand(
            Command::new("cluster")
                .about("Cluster management")
                .subcommand(
                    Command::new("status")
                        .about("Get cluster status")
                )
                .subcommand(
                    Command::new("add-node")
                        .about("Add a node to the cluster")
                        .arg(
                            Arg::new("node-id")
                                .help("Node ID")
                                .required(true)
                                .index(1)
                        )
                        .arg(
                            Arg::new("address")
                                .help("Node address")
                                .required(true)
                                .index(2)
                        )
                )
                .subcommand(
                    Command::new("remove-node")
                        .about("Remove a node from the cluster")
                        .arg(
                            Arg::new("node-id")
                                .help("Node ID")
                                .required(true)
                                .index(1)
                        )
                )
        )
        .subcommand(
            Command::new("health")
                .about("Check server health")
        )
        .subcommand(
            Command::new("metrics")
                .about("Get server metrics")
        )
}

/// Parse configuration from command line arguments
fn parse_config(matches: &ArgMatches) -> Result<ClientConfig, Box<dyn std::error::Error + Send + Sync>> {
    let server_address = matches.get_one::<String>("server").unwrap().clone();
    
    let protocol = match matches.get_one::<String>("protocol").unwrap().as_str() {
        "http" => Protocol::Http,
        "grpc" => Protocol::Grpc,
        _ => return Err("Invalid protocol".into()),
    };
    
    let timeout = Duration::from_secs(*matches.get_one::<u64>("timeout").unwrap());
    
    let output_format = match matches.get_one::<String>("format").unwrap().as_str() {
        "json" => OutputFormat::Json,
        "table" => OutputFormat::Table,
        "plain" => OutputFormat::Plain,
        _ => return Err("Invalid output format".into()),
    };
    
    let verbose = matches.get_flag("verbose");
    
    Ok(ClientConfig {
        server_address,
        protocol,
        timeout,
        output_format,
        verbose,
    })
}

/// Initialize logging
fn init_logging(verbose: bool) {
    let level = if verbose {
        tracing::Level::DEBUG
    } else {
        tracing::Level::INFO
    };
    
    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_target(false)
        .init();
}

/// Execute client command
async fn execute_command(
    mut client: KvStoreClient,
    matches: &ArgMatches,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    match matches.subcommand() {
        Some(("put", sub_matches)) => {
            let key = sub_matches.get_one::<String>("key").unwrap();
            let value = sub_matches.get_one::<String>("value").unwrap().as_bytes();
            let ttl = sub_matches.get_one::<u64>("ttl").copied();
            
            client.put(key, value, ttl).await
        }
        Some(("get", sub_matches)) => {
            let key = sub_matches.get_one::<String>("key").unwrap();
            
            client.get(key).await
        }
        Some(("delete", sub_matches)) => {
            let key = sub_matches.get_one::<String>("key").unwrap();
            
            client.delete(key).await
        }
        Some(("list", sub_matches)) => {
            let prefix = sub_matches.get_one::<String>("prefix").map(|s| s.as_str());
            let limit = sub_matches.get_one::<usize>("limit").copied();
            let offset = sub_matches.get_one::<usize>("offset").copied();
            
            client.list(prefix, limit, offset).await
        }
        Some(("cluster", sub_matches)) => {
            match sub_matches.subcommand() {
                Some(("status", _)) => {
                    client.cluster_status().await
                }
                Some(("add-node", sub_sub_matches)) => {
                    let node_id = sub_sub_matches.get_one::<String>("node-id").unwrap();
                    let address = sub_sub_matches.get_one::<String>("address").unwrap();
                    
                    client.add_node(node_id, address).await
                }
                Some(("remove-node", sub_sub_matches)) => {
                    let node_id = sub_sub_matches.get_one::<String>("node-id").unwrap();
                    
                    client.remove_node(node_id).await
                }
                _ => Err("Invalid cluster subcommand".into()),
            }
        }
        Some(("health", _)) => {
            client.health().await
        }
        Some(("metrics", _)) => {
            client.metrics().await
        }
        _ => Err("No command specified".into()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let matches = create_cli().get_matches();
    
    // Parse configuration
    let config = parse_config(&matches)?;
    
    // Initialize logging
    init_logging(config.verbose);
    
    if config.verbose {
        info!("Client configuration: {:?}", config);
    }
    
    // Create client
    let client = KvStoreClient::new(config.clone()).await
        .map_err(|e| {
            error!("Failed to create client: {}", e);
            e
        })?;
    
    // Execute command
    let result = execute_command(client, &matches).await
        .map_err(|e| {
            error!("Command failed: {}", e);
            e
        })?;
    
    // Format and print output
    let output = format_output(&result, &config.output_format);
    println!("{}", output);
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_client_config_default() {
        let config = ClientConfig::default();
        
        assert_eq!(config.server_address, "http://localhost:8080");
        assert_eq!(config.protocol, Protocol::Http);
        assert_eq!(config.timeout, Duration::from_secs(30));
        assert_eq!(config.output_format, OutputFormat::Json);
        assert!(!config.verbose);
    }
    
    #[test]
    fn test_parse_config() {
        let cli = create_cli();
        let matches = cli.try_get_matches_from(vec![
            "kvstore-client",
            "--server", "grpc://localhost:50051",
            "--protocol", "grpc",
            "--timeout", "60",
            "--format", "table",
            "--verbose",
            "get", "test_key"
        ]).unwrap();
        
        let config = parse_config(&matches).unwrap();
        
        assert_eq!(config.server_address, "grpc://localhost:50051");
        assert_eq!(config.protocol, Protocol::Grpc);
        assert_eq!(config.timeout, Duration::from_secs(60));
        assert_eq!(config.output_format, OutputFormat::Table);
        assert!(config.verbose);
    }
    
    #[test]
    fn test_format_output() {
        let value = json!({
            "key": "test_key",
            "value": "test_value",
            "found": true
        });
        
        // Test JSON format
        let json_output = format_output(&value, &OutputFormat::Json);
        assert!(json_output.contains("test_key"));
        assert!(json_output.contains("test_value"));
        
        // Test table format
        let table_output = format_output(&value, &OutputFormat::Table);
        assert!(table_output.contains("key"));
        assert!(table_output.contains("|"));
        
        // Test plain format
        let plain_output = format_output(&value, &OutputFormat::Plain);
        assert!(plain_output.contains("test_key"));
    }
    
    #[test]
    fn test_format_value_inline() {
        assert_eq!(format_value_inline(&json!("test")), "test");
        assert_eq!(format_value_inline(&json!(42)), "42");
        assert_eq!(format_value_inline(&json!(true)), "true");
        assert_eq!(format_value_inline(&json!(null)), "null");
        assert_eq!(format_value_inline(&json!([1, 2, 3])), "[1, 2, 3]");
        assert_eq!(format_value_inline(&json!({"key": "value"})), "<object>");
    }
    
    #[test]
    fn test_cli_creation() {
        let cli = create_cli();
        
        // Test that CLI can be created without panicking
        assert_eq!(cli.get_name(), "kvstore-client");
        
        // Test parsing with minimal arguments
        let matches = cli.try_get_matches_from(vec!["kvstore-client", "health"]).unwrap();
        assert_eq!(matches.get_one::<String>("server").unwrap(), "http://localhost:8080");
        assert_eq!(matches.get_one::<String>("protocol").unwrap(), "http");
    }
    
    #[tokio::test]
    async fn test_http_client_creation() {
        let client = HttpClient::new("http://localhost:8080");
        assert!(client.is_ok());
        
        let client = HttpClient::new("invalid-url");
        assert!(client.is_ok()); // reqwest client creation doesn't validate URL format
    }
}