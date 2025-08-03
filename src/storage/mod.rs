//! # 存储模块
//!
//! 本模块提供了分布式键值存储系统的持久化存储功能，基于 sled 嵌入式数据库
//! 实现高性能的数据持久化。存储模块负责管理键值数据、Raft 日志、元数据
//! 和快照等各种类型的数据。
//!
//! ## 功能特性
//!
//! - **多树存储**: 使用不同的树结构分别存储键值数据、日志和元数据
//! - **事务支持**: 提供 ACID 事务保证，确保数据一致性
//! - **高性能**: 基于 sled 数据库，提供高吞吐量和低延迟
//! - **持久化**: 所有数据自动持久化到磁盘，支持崩溃恢复
//! - **压缩**: 支持数据压缩，减少存储空间占用
//! - **快照**: 支持创建和恢复数据快照
//!
//! ## 存储结构
//!
//! 存储系统使用三个独立的树结构：
//! - **kv_tree**: 存储用户的键值数据
//! - **log_tree**: 存储 Raft 日志条目
//! - **meta_tree**: 存储系统元数据（如当前任期、投票信息等）
//!
//! ## 核心组件
//!
//! - [`Storage`]: 主要的存储引擎实现
//! - [`StorageError`]: 存储相关的错误类型
//! - [`log`]: Raft 日志存储子模块
//! - [`snapshot`]: 快照管理子模块
//!
//! ## 使用示例
//!
//! ```rust
//! use distributed_kv_store::storage::Storage;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // 创建存储实例
//!     let storage = Storage::new("./data").await?;
//!
//!     // 存储键值对
//!     storage.put("user:1".to_string(), "Alice".to_string()).await?;
//!
//!     // 读取数据
//!     let value = storage.get("user:1").await?;
//!     println!("读取到的值: {:?}", value);
//!
//!     // 删除数据
//!     storage.delete("user:1").await?;
//!
//!     Ok(())
//! }
//! ```

use anyhow::Result;
use serde::{Deserialize, Serialize};
use sled::{Db, Tree};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

pub mod log;
pub mod snapshot;

use crate::{LogIndex, Term};

/// 存储相关的错误类型
///
/// 定义了存储操作过程中可能遇到的各种错误情况，包括数据库错误、
/// 序列化错误、IO 错误等。所有错误都实现了标准的 Error trait，
/// 可以与其他错误类型组合使用。
///
/// # 错误分类
///
/// - **数据库错误**: sled 数据库操作失败
/// - **序列化错误**: 数据序列化/反序列化失败
/// - **IO 错误**: 文件系统操作失败
/// - **逻辑错误**: 键不存在、数据格式无效等
///
/// # 使用示例
///
/// ```rust
/// use distributed_kv_store::storage::{Storage, StorageError};
///
/// async fn handle_storage_operation() {
///     let storage = Storage::new("./data").await.unwrap();
///     
///     match storage.get("nonexistent_key").await {
///         Ok(value) => println!("找到值: {:?}", value),
///         Err(StorageError::KeyNotFound(key)) => {
///             println!("键 '{}' 不存在", key);
///         }
///         Err(e) => {
///             eprintln!("存储错误: {}", e);
///         }
///     }
/// }
/// ```
#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("Database error: {0}")]
    Database(#[from] sled::Error),
    
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),
    
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Key not found: {0}")]
    KeyNotFound(String),
    
    #[error("Invalid data format")]
    InvalidFormat,
}

pub type StorageResult<T> = Result<T, StorageError>;

/// Persistent storage engine
#[derive(Clone)]
pub struct Storage {
    db: Arc<Db>,
    kv_tree: Arc<Tree>,
    log_tree: Arc<Tree>,
    meta_tree: Arc<Tree>,
    state: Arc<RwLock<StorageState>>,
}

/// Storage state metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageState {
    pub current_term: Term,
    pub voted_for: Option<String>,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
}

impl Default for StorageState {
    fn default() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            commit_index: 0,
            last_applied: 0,
        }
    }
}

impl Storage {
    /// Create a new storage instance
    pub async fn new<P: AsRef<Path>>(data_dir: P) -> StorageResult<Self> {
        let db = sled::open(data_dir)?;
        
        let kv_tree = Arc::new(db.open_tree("kv")?);
        let log_tree = Arc::new(db.open_tree("log")?);
        let meta_tree = Arc::new(db.open_tree("meta")?);
        
        let state = Arc::new(RwLock::new(Self::load_state(&meta_tree)?));
        
        Ok(Self {
            db: Arc::new(db),
            kv_tree,
            log_tree,
            meta_tree,
            state,
        })
    }
    
    /// Load storage state from metadata tree
    fn load_state(meta_tree: &Tree) -> StorageResult<StorageState> {
        match meta_tree.get("state")? {
            Some(data) => Ok(bincode::deserialize(&data)?),
            None => Ok(StorageState::default()),
        }
    }
    
    /// Save storage state to metadata tree
    async fn save_state(&self) -> StorageResult<()> {
        let state = self.state.read().await;
        let data = bincode::serialize(&*state)?;
        self.meta_tree.insert("state", data)?;
        self.db.flush_async().await?;
        Ok(())
    }
    
    /// Get a value by key
    pub async fn get(&self, key: &str) -> StorageResult<Option<String>> {
        match self.kv_tree.get(key)? {
            Some(value) => {
                let value_str = String::from_utf8(value.to_vec())
                    .map_err(|_| StorageError::InvalidFormat)?;
                Ok(Some(value_str))
            }
            None => Ok(None),
        }
    }
    
    /// Put a key-value pair
    pub async fn put(&self, key: &str, value: &str) -> StorageResult<()> {
        self.kv_tree.insert(key, value.as_bytes())?;
        self.db.flush_async().await?;
        Ok(())
    }
    
    /// Delete a key
    pub async fn delete(&self, key: &str) -> StorageResult<bool> {
        let existed = self.kv_tree.remove(key)?.is_some();
        self.db.flush_async().await?;
        Ok(existed)
    }
    
    /// List keys with optional prefix
    pub async fn list(&self, prefix: Option<&str>, limit: Option<usize>) -> StorageResult<Vec<(String, String)>> {
        let mut results = Vec::new();
        let iter = match prefix {
            Some(p) => self.kv_tree.scan_prefix(p),
            None => self.kv_tree.iter(),
        };
        
        for (i, item) in iter.enumerate() {
            if let Some(limit) = limit {
                if i >= limit {
                    break;
                }
            }
            
            let (key, value) = item?;
            let key_str = String::from_utf8(key.to_vec())
                .map_err(|_| StorageError::InvalidFormat)?;
            let value_str = String::from_utf8(value.to_vec())
                .map_err(|_| StorageError::InvalidFormat)?;
            results.push((key_str, value_str));
        }
        
        Ok(results)
    }
    
    /// Get current term
    pub async fn get_current_term(&self) -> Term {
        self.state.read().await.current_term
    }
    
    /// Set current term
    pub async fn set_current_term(&self, term: Term) -> StorageResult<()> {
        {
            let mut state = self.state.write().await;
            state.current_term = term;
            state.voted_for = None; // Reset vote when term changes
        }
        self.save_state().await
    }
    
    /// Get voted for candidate
    pub async fn get_voted_for(&self) -> Option<String> {
        self.state.read().await.voted_for.clone()
    }
    
    /// Set voted for candidate
    pub async fn set_voted_for(&self, candidate: Option<String>) -> StorageResult<()> {
        {
            let mut state = self.state.write().await;
            state.voted_for = candidate;
        }
        self.save_state().await
    }
    
    /// Get commit index
    pub async fn get_commit_index(&self) -> LogIndex {
        self.state.read().await.commit_index
    }
    
    /// Set commit index
    pub async fn set_commit_index(&self, index: LogIndex) -> StorageResult<()> {
        {
            let mut state = self.state.write().await;
            state.commit_index = index;
        }
        self.save_state().await
    }
    
    /// Get last applied index
    pub async fn get_last_applied(&self) -> LogIndex {
        self.state.read().await.last_applied
    }
    
    /// Set last applied index
    pub async fn set_last_applied(&self, index: LogIndex) -> StorageResult<()> {
        {
            let mut state = self.state.write().await;
            state.last_applied = index;
        }
        self.save_state().await
    }
    
    /// Get log tree reference for log operations
    pub fn log_tree(&self) -> &Tree {
        &self.log_tree
    }
    
    /// Flush all pending writes
    pub async fn flush(&self) -> StorageResult<()> {
        self.db.flush_async().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    async fn create_test_storage() -> (Storage, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::new(temp_dir.path()).await.unwrap();
        (storage, temp_dir)
    }
    
    #[tokio::test]
    async fn test_kv_operations() {
        let (storage, _temp_dir) = create_test_storage().await;
        
        // Test put and get
        storage.put("key1", "value1").await.unwrap();
        let value = storage.get("key1").await.unwrap();
        assert_eq!(value, Some("value1".to_string()));
        
        // Test get non-existent key
        let value = storage.get("nonexistent").await.unwrap();
        assert_eq!(value, None);
        
        // Test delete
        let deleted = storage.delete("key1").await.unwrap();
        assert!(deleted);
        
        let value = storage.get("key1").await.unwrap();
        assert_eq!(value, None);
    }
    
    #[tokio::test]
    async fn test_list_operations() {
        let (storage, _temp_dir) = create_test_storage().await;
        
        // Insert test data
        storage.put("app:key1", "value1").await.unwrap();
        storage.put("app:key2", "value2").await.unwrap();
        storage.put("other:key3", "value3").await.unwrap();
        
        // Test list with prefix
        let results = storage.list(Some("app:"), None).await.unwrap();
        assert_eq!(results.len(), 2);
        
        // Test list with limit
        let results = storage.list(None, Some(2)).await.unwrap();
        assert_eq!(results.len(), 2);
    }
    
    #[tokio::test]
    async fn test_state_operations() {
        let (storage, _temp_dir) = create_test_storage().await;
        
        // Test term operations
        assert_eq!(storage.get_current_term().await, 0);
        storage.set_current_term(5).await.unwrap();
        assert_eq!(storage.get_current_term().await, 5);
        
        // Test voted_for operations
        assert_eq!(storage.get_voted_for().await, None);
        storage.set_voted_for(Some("node1".to_string())).await.unwrap();
        assert_eq!(storage.get_voted_for().await, Some("node1".to_string()));
    }
}