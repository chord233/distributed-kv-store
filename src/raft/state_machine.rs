//! # Raft 状态机实现模块
//!
//! 本模块实现了 Raft 算法的状态机组件，负责将已提交的日志条目应用到
//! 实际的业务状态中。状态机确保所有节点以相同的顺序执行相同的命令，
//! 从而维护分布式系统的一致性。
//!
//! ## 功能特性
//!
//! - **命令执行**: 执行键值存储操作（PUT、DELETE）
//! - **配置变更**: 处理集群成员变更操作
//! - **状态持久化**: 将状态变更持久化到存储层
//! - **快照支持**: 创建和恢复状态机快照
//! - **幂等性保证**: 确保重复应用相同日志条目的安全性
//!
//! ## 核心组件
//!
//! - [`StateMachine`]: 状态机主体实现
//! - [`StateMachineCommand`]: 状态机支持的命令类型
//! - [`StateMachineResult`]: 命令执行结果
//! - [`ConfigChangeType`]: 配置变更类型
//!
//! ## 命令类型
//!
//! 状态机支持以下类型的命令：
//! - **Put**: 设置键值对
//! - **Delete**: 删除指定键
//! - **ConfigChange**: 集群配置变更
//! - **NoOp**: 空操作（用于领导者确认）
//!
//! ## 使用示例
//!
//! ```rust
//! use distributed_kv_store::raft::state_machine::{
//!     StateMachine, StateMachineCommand
//! };
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let storage = Arc::new(Storage::new("./data").await?);
//!     let mut state_machine = StateMachine::new(storage).await?;
//!
//!     // 应用 PUT 命令
//!     let command = StateMachineCommand::Put {
//!         key: "user:1".to_string(),
//!         value: "Alice".to_string(),
//!     };
//!
//!     let result = state_machine.apply_command(1, command).await?;
//!     println!("命令执行结果: {:?}", result);
//!
//!     Ok(())
//! }
//! ```

use super::{RaftError, RaftResult};
use crate::storage::log::{LogEntry, LogEntryType};
use crate::storage::Storage;
use crate::{LogIndex, Term};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// 状态机命令类型
///
/// 定义了状态机可以执行的所有命令类型。这些命令通过 Raft 日志复制
/// 到所有节点，并按相同顺序应用，确保集群状态的一致性。
///
/// # 命令分类
///
/// - **数据操作**: Put、Delete - 修改键值存储的数据
/// - **配置操作**: ConfigChange - 修改集群配置
/// - **控制操作**: NoOp - 用于领导者确认和心跳
///
/// # 序列化
///
/// 所有命令都实现了 Serialize 和 Deserialize trait，
/// 可以安全地在网络间传输和持久化存储。
///
/// # 使用示例
///
/// ```rust
/// use distributed_kv_store::raft::state_machine::{
///     StateMachineCommand, ConfigChangeType
/// };
///
/// // 创建 PUT 命令
/// let put_cmd = StateMachineCommand::Put {
///     key: "config:timeout".to_string(),
///     value: "30".to_string(),
/// };
///
/// // 创建删除命令
/// let delete_cmd = StateMachineCommand::Delete {
///     key: "temp:session".to_string(),
/// };
///
/// // 创建配置变更命令
/// let config_cmd = StateMachineCommand::ConfigChange {
///     change_type: ConfigChangeType::AddNode,
///     node_id: "node-3".to_string(),
///     address: "192.168.1.103:8080".to_string(),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StateMachineCommand {
    /// Put a key-value pair
    Put { key: String, value: String },
    /// Delete a key
    Delete { key: String },
    /// Configuration change
    ConfigChange { 
        change_type: ConfigChangeType,
        node_id: String,
        address: String,
    },
    /// No-op command
    NoOp,
}

/// Configuration change types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfigChangeType {
    AddNode,
    RemoveNode,
}

/// State machine result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StateMachineResult {
    /// Success with optional value
    Success { value: Option<String> },
    /// Error with message
    Error { message: String },
}

/// Raft state machine
pub struct StateMachine {
    /// Persistent storage
    storage: Arc<Storage>,
    /// Last applied log index
    last_applied: Arc<RwLock<LogIndex>>,
    /// In-memory cache for performance
    cache: Arc<RwLock<HashMap<String, String>>>,
    /// Configuration state
    config: Arc<RwLock<ClusterConfig>>,
}

/// Cluster configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Current cluster members
    pub members: HashMap<String, String>, // node_id -> address
    /// Configuration index (log index where this config was applied)
    pub config_index: LogIndex,
    /// Configuration term
    pub config_term: Term,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            members: HashMap::new(),
            config_index: 0,
            config_term: 0,
        }
    }
}

impl StateMachine {
    /// Create a new state machine
    pub async fn new(storage: Arc<Storage>) -> RaftResult<Self> {
        let last_applied = storage.get_last_applied().await;
        
        // Load configuration from storage
        let config = Self::load_config(&storage).await.unwrap_or_default();
        
        // Initialize cache by loading all data
        let cache = Self::load_cache(&storage).await?;
        
        Ok(Self {
            storage,
            last_applied: Arc::new(RwLock::new(last_applied)),
            cache: Arc::new(RwLock::new(cache)),
            config: Arc::new(RwLock::new(config)),
        })
    }
    
    /// Load configuration from storage
    async fn load_config(storage: &Storage) -> Option<ClusterConfig> {
        match storage.get("__cluster_config").await {
            Ok(Some(data)) => {
                match serde_json::from_str::<ClusterConfig>(&data) {
                    Ok(config) => Some(config),
                    Err(e) => {
                        warn!("Failed to deserialize cluster config: {}", e);
                        None
                    }
                }
            }
            _ => None,
        }
    }
    
    /// Save configuration to storage
    async fn save_config(&self, config: &ClusterConfig) -> RaftResult<()> {
        let data = serde_json::to_string(config)
            .map_err(|e| RaftError::Network(format!("Serialization error: {}", e)))?;
        
        self.storage.put("__cluster_config", &data).await
            .map_err(RaftError::Storage)?;
        
        Ok(())
    }
    
    /// Load cache from storage
    async fn load_cache(storage: &Storage) -> RaftResult<HashMap<String, String>> {
        let mut cache = HashMap::new();
        
        match storage.list(None, None).await {
            Ok(pairs) => {
                for (key, value) in pairs {
                    // Skip internal keys
                    if !key.starts_with("__") {
                        cache.insert(key, value);
                    }
                }
            }
            Err(e) => {
                warn!("Failed to load cache from storage: {}", e);
            }
        }
        
        Ok(cache)
    }
    
    /// Apply a log entry to the state machine
    pub async fn apply_entry(&self, entry: &LogEntry) -> RaftResult<StateMachineResult> {
        let current_last_applied = *self.last_applied.read().await;
        
        // Ensure we apply entries in order
        if entry.index != current_last_applied + 1 {
            return Err(RaftError::InvalidLogIndex(entry.index));
        }
        
        let result = match entry.entry_type {
            LogEntryType::Application => {
                self.apply_application_entry(entry).await?
            }
            LogEntryType::Configuration => {
                self.apply_configuration_entry(entry).await?
            }
            LogEntryType::NoOp => {
                StateMachineResult::Success { value: None }
            }
        };
        
        // Update last applied index
        {
            let mut last_applied = self.last_applied.write().await;
            *last_applied = entry.index;
        }
        
        // Persist last applied index
        self.storage.set_last_applied(entry.index).await
            .map_err(RaftError::Storage)?;
        
        debug!("Applied log entry {} to state machine", entry.index);
        
        Ok(result)
    }
    
    /// Apply an application log entry
    async fn apply_application_entry(&self, entry: &LogEntry) -> RaftResult<StateMachineResult> {
        let command: StateMachineCommand = bincode::deserialize(&entry.data)
            .map_err(|e| RaftError::Network(format!("Deserialization error: {}", e)))?;
        
        match command {
            StateMachineCommand::Put { key, value } => {
                self.apply_put(&key, &value).await
            }
            StateMachineCommand::Delete { key } => {
                self.apply_delete(&key).await
            }
            StateMachineCommand::NoOp => {
                Ok(StateMachineResult::Success { value: None })
            }
            StateMachineCommand::ConfigChange { .. } => {
                // Config changes should be in configuration entries
                Err(RaftError::InvalidStateTransition)
            }
        }
    }
    
    /// Apply a configuration log entry
    async fn apply_configuration_entry(&self, entry: &LogEntry) -> RaftResult<StateMachineResult> {
        let command: StateMachineCommand = bincode::deserialize(&entry.data)
            .map_err(|e| RaftError::Network(format!("Deserialization error: {}", e)))?;
        
        match command {
            StateMachineCommand::ConfigChange { change_type, node_id, address } => {
                self.apply_config_change(change_type, &node_id, &address, entry.index, entry.term).await
            }
            _ => {
                // Only config changes should be in configuration entries
                Err(RaftError::InvalidStateTransition)
            }
        }
    }
    
    /// Apply a put operation
    async fn apply_put(&self, key: &str, value: &str) -> RaftResult<StateMachineResult> {
        // Update cache
        {
            let mut cache = self.cache.write().await;
            cache.insert(key.to_string(), value.to_string());
        }
        
        // Persist to storage
        match self.storage.put(key, value).await {
            Ok(_) => {
                debug!("Applied PUT: {} = {}", key, value);
                Ok(StateMachineResult::Success { value: None })
            }
            Err(e) => {
                error!("Failed to persist PUT operation: {}", e);
                Ok(StateMachineResult::Error {
                    message: format!("Storage error: {}", e),
                })
            }
        }
    }
    
    /// Apply a delete operation
    async fn apply_delete(&self, key: &str) -> RaftResult<StateMachineResult> {
        // Get old value for result
        let old_value = {
            let cache = self.cache.read().await;
            cache.get(key).cloned()
        };
        
        // Update cache
        {
            let mut cache = self.cache.write().await;
            cache.remove(key);
        }
        
        // Persist to storage
        match self.storage.delete(key).await {
            Ok(existed) => {
                debug!("Applied DELETE: {} (existed: {})", key, existed);
                Ok(StateMachineResult::Success { value: old_value })
            }
            Err(e) => {
                error!("Failed to persist DELETE operation: {}", e);
                Ok(StateMachineResult::Error {
                    message: format!("Storage error: {}", e),
                })
            }
        }
    }
    
    /// Apply a configuration change
    async fn apply_config_change(
        &self,
        change_type: ConfigChangeType,
        node_id: &str,
        address: &str,
        config_index: LogIndex,
        config_term: Term,
    ) -> RaftResult<StateMachineResult> {
        let mut config = self.config.write().await;
        
        match change_type {
            ConfigChangeType::AddNode => {
                config.members.insert(node_id.to_string(), address.to_string());
                info!("Added node {} at {} to cluster configuration", node_id, address);
            }
            ConfigChangeType::RemoveNode => {
                config.members.remove(node_id);
                info!("Removed node {} from cluster configuration", node_id);
            }
        }
        
        config.config_index = config_index;
        config.config_term = config_term;
        
        // Persist configuration
        if let Err(e) = self.save_config(&config).await {
            error!("Failed to persist configuration change: {}", e);
            return Ok(StateMachineResult::Error {
                message: format!("Failed to persist config: {}", e),
            });
        }
        
        Ok(StateMachineResult::Success { value: None })
    }
    
    /// Get a value from the state machine
    pub async fn get(&self, key: &str) -> Option<String> {
        let cache = self.cache.read().await;
        cache.get(key).cloned()
    }
    
    /// List keys with optional prefix
    pub async fn list(&self, prefix: Option<&str>, limit: Option<usize>) -> Vec<(String, String)> {
        let cache = self.cache.read().await;
        let mut results: Vec<_> = cache
            .iter()
            .filter(|(key, _)| {
                if let Some(p) = prefix {
                    key.starts_with(p)
                } else {
                    true
                }
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        
        results.sort_by(|a, b| a.0.cmp(&b.0));
        
        if let Some(limit) = limit {
            results.truncate(limit);
        }
        
        results
    }
    
    /// Get current cluster configuration
    pub async fn get_config(&self) -> ClusterConfig {
        self.config.read().await.clone()
    }
    
    /// Get last applied index
    pub async fn get_last_applied(&self) -> LogIndex {
        *self.last_applied.read().await
    }
    
    /// Create a snapshot of the current state
    pub async fn create_snapshot(&self) -> RaftResult<StateMachineSnapshot> {
        let cache = self.cache.read().await;
        let config = self.config.read().await;
        let last_applied = *self.last_applied.read().await;
        
        Ok(StateMachineSnapshot {
            data: cache.clone(),
            config: config.clone(),
            last_applied,
        })
    }
    
    /// Restore from a snapshot
    pub async fn restore_from_snapshot(&self, snapshot: StateMachineSnapshot) -> RaftResult<()> {
        // Update cache
        {
            let mut cache = self.cache.write().await;
            *cache = snapshot.data;
        }
        
        // Update configuration
        {
            let mut config = self.config.write().await;
            *config = snapshot.config.clone();
        }
        
        // Update last applied
        {
            let mut last_applied = self.last_applied.write().await;
            *last_applied = snapshot.last_applied;
        }
        
        // Persist everything to storage
        let cache = self.cache.read().await;
        for (key, value) in cache.iter() {
            self.storage.put(key, value).await
                .map_err(RaftError::Storage)?;
        }
        
        self.save_config(&snapshot.config).await?;
        
        self.storage.set_last_applied(snapshot.last_applied).await
            .map_err(RaftError::Storage)?;
        
        info!("Restored state machine from snapshot at index {}", snapshot.last_applied);
        
        Ok(())
    }
    
    /// Get state machine statistics
    pub async fn get_stats(&self) -> StateMachineStats {
        let cache = self.cache.read().await;
        let config = self.config.read().await;
        let last_applied = *self.last_applied.read().await;
        
        StateMachineStats {
            key_count: cache.len(),
            last_applied,
            config_index: config.config_index,
            config_term: config.config_term,
            cluster_size: config.members.len(),
        }
    }
}

/// State machine snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMachineSnapshot {
    pub data: HashMap<String, String>,
    pub config: ClusterConfig,
    pub last_applied: LogIndex,
}

/// State machine statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMachineStats {
    pub key_count: usize,
    pub last_applied: LogIndex,
    pub config_index: LogIndex,
    pub config_term: Term,
    pub cluster_size: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::log::LogEntry;
    use tempfile::TempDir;
    
    async fn create_test_state_machine() -> (StateMachine, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(Storage::new(temp_dir.path()).await.unwrap());
        let state_machine = StateMachine::new(storage).await.unwrap();
        (state_machine, temp_dir)
    }
    
    fn create_put_entry(index: LogIndex, term: Term, key: &str, value: &str) -> LogEntry {
        let command = StateMachineCommand::Put {
            key: key.to_string(),
            value: value.to_string(),
        };
        let data = bincode::serialize(&command).unwrap();
        
        LogEntry {
            term,
            index,
            data,
            entry_type: LogEntryType::Application,
        }
    }
    
    fn create_delete_entry(index: LogIndex, term: Term, key: &str) -> LogEntry {
        let command = StateMachineCommand::Delete {
            key: key.to_string(),
        };
        let data = bincode::serialize(&command).unwrap();
        
        LogEntry {
            term,
            index,
            data,
            entry_type: LogEntryType::Application,
        }
    }
    
    #[tokio::test]
    async fn test_state_machine_creation() {
        let (state_machine, _temp_dir) = create_test_state_machine().await;
        
        assert_eq!(state_machine.get_last_applied().await, 0);
        let stats = state_machine.get_stats().await;
        assert_eq!(stats.key_count, 0);
    }
    
    #[tokio::test]
    async fn test_apply_put_entry() {
        let (state_machine, _temp_dir) = create_test_state_machine().await;
        
        let entry = create_put_entry(1, 1, "key1", "value1");
        let result = state_machine.apply_entry(&entry).await.unwrap();
        
        match result {
            StateMachineResult::Success { .. } => {},
            _ => panic!("Expected success result"),
        }
        
        // Check that value was applied
        let value = state_machine.get("key1").await;
        assert_eq!(value, Some("value1".to_string()));
        
        assert_eq!(state_machine.get_last_applied().await, 1);
    }
    
    #[tokio::test]
    async fn test_apply_delete_entry() {
        let (state_machine, _temp_dir) = create_test_state_machine().await;
        
        // First put a value
        let put_entry = create_put_entry(1, 1, "key1", "value1");
        state_machine.apply_entry(&put_entry).await.unwrap();
        
        // Then delete it
        let delete_entry = create_delete_entry(2, 1, "key1");
        let result = state_machine.apply_entry(&delete_entry).await.unwrap();
        
        match result {
            StateMachineResult::Success { value } => {
                assert_eq!(value, Some("value1".to_string()));
            }
            _ => panic!("Expected success result"),
        }
        
        // Check that value was deleted
        let value = state_machine.get("key1").await;
        assert_eq!(value, None);
        
        assert_eq!(state_machine.get_last_applied().await, 2);
    }
    
    #[tokio::test]
    async fn test_list_operations() {
        let (state_machine, _temp_dir) = create_test_state_machine().await;
        
        // Add some test data
        let entries = vec![
            create_put_entry(1, 1, "app:key1", "value1"),
            create_put_entry(2, 1, "app:key2", "value2"),
            create_put_entry(3, 1, "other:key3", "value3"),
        ];
        
        for entry in entries {
            state_machine.apply_entry(&entry).await.unwrap();
        }
        
        // Test list with prefix
        let results = state_machine.list(Some("app:"), None).await;
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], ("app:key1".to_string(), "value1".to_string()));
        assert_eq!(results[1], ("app:key2".to_string(), "value2".to_string()));
        
        // Test list with limit
        let results = state_machine.list(None, Some(2)).await;
        assert_eq!(results.len(), 2);
    }
    
    #[tokio::test]
    async fn test_snapshot_and_restore() {
        let (state_machine, _temp_dir) = create_test_state_machine().await;
        
        // Add some test data
        let entries = vec![
            create_put_entry(1, 1, "key1", "value1"),
            create_put_entry(2, 1, "key2", "value2"),
        ];
        
        for entry in entries {
            state_machine.apply_entry(&entry).await.unwrap();
        }
        
        // Create snapshot
        let snapshot = state_machine.create_snapshot().await.unwrap();
        assert_eq!(snapshot.last_applied, 2);
        assert_eq!(snapshot.data.len(), 2);
        
        // Create new state machine and restore
        let temp_dir2 = TempDir::new().unwrap();
        let storage2 = Arc::new(Storage::new(temp_dir2.path()).await.unwrap());
        let state_machine2 = StateMachine::new(storage2).await.unwrap();
        
        state_machine2.restore_from_snapshot(snapshot).await.unwrap();
        
        // Verify restored data
        assert_eq!(state_machine2.get_last_applied().await, 2);
        assert_eq!(state_machine2.get("key1").await, Some("value1".to_string()));
        assert_eq!(state_machine2.get("key2").await, Some("value2".to_string()));
    }
    
    #[tokio::test]
    async fn test_out_of_order_application() {
        let (state_machine, _temp_dir) = create_test_state_machine().await;
        
        // Try to apply entry with index 2 before index 1
        let entry = create_put_entry(2, 1, "key1", "value1");
        let result = state_machine.apply_entry(&entry).await;
        
        assert!(result.is_err());
        match result.unwrap_err() {
            RaftError::InvalidLogIndex(2) => {},
            _ => panic!("Expected InvalidLogIndex error"),
        }
    }
}