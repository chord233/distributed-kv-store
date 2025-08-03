//! # Raft 日志存储实现模块
//!
//! 本模块实现了 Raft 算法的日志存储功能，负责持久化存储 Raft 日志条目。
//! 日志存储是 Raft 算法的核心组件，确保所有操作的持久性和一致性。
//!
//! ## 功能特性
//!
//! - **持久化存储**: 将日志条目持久化到磁盘，确保崩溃恢复
//! - **高效索引**: 使用日志索引快速定位和检索日志条目
//! - **批量操作**: 支持批量追加和删除操作，提高性能
//! - **类型安全**: 强类型的日志条目，防止数据损坏
//! - **压缩支持**: 支持日志压缩，减少存储空间
//!
//! ## 日志条目类型
//!
//! - **Application**: 应用数据条目，包含用户操作
//! - **Configuration**: 配置变更条目，用于集群成员变更
//! - **NoOp**: 空操作条目，用于领导者确认
//!
//! ## 核心组件
//!
//! - [`LogEntry`]: 单个日志条目的表示
//! - [`LogEntryType`]: 日志条目的类型枚举
//! - [`LogStorage`]: 日志存储管理器
//!
//! ## 存储格式
//!
//! 日志条目使用 bincode 进行序列化，键格式为 8 字节的大端序索引，
//! 确保按索引顺序存储和检索。
//!
//! ## 使用示例
//!
//! ```rust
//! use distributed_kv_store::storage::log::{
//!     LogStorage, LogEntry, LogEntryType
//! };
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let db = sled::open("./log_data")?;
//!     let tree = db.open_tree("raft_log")?;
//!     let log_storage = LogStorage::new(tree);
//!
//!     // 创建日志条目
//!     let entry = LogEntry {
//!         term: 1,
//!         index: 1,
//!         data: b"set key value".to_vec(),
//!         entry_type: LogEntryType::Application,
//!     };
//!
//!     // 追加日志条目
//!     log_storage.append_entry(entry)?;
//!
//!     // 读取日志条目
//!     let retrieved = log_storage.get_entry(1)?;
//!     println!("日志条目: {:?}", retrieved);
//!
//!     Ok(())
//! }
//! ```

use super::{StorageError, StorageResult};
use crate::{LogIndex, Term};
use serde::{Deserialize, Serialize};
use sled::Tree;

/// Raft 日志条目
///
/// 表示 Raft 算法中的单个日志条目，包含了条目的所有必要信息。
/// 每个日志条目都有唯一的索引和任期，确保在分布式环境中的一致性。
///
/// # 字段说明
///
/// - `term`: 创建此条目时的领导者任期
/// - `index`: 日志条目在日志中的位置索引
/// - `data`: 条目包含的实际数据（序列化后的命令）
/// - `entry_type`: 条目类型，区分不同种类的操作
///
/// # 序列化
///
/// LogEntry 实现了 Serialize 和 Deserialize trait，可以安全地
/// 持久化到存储系统中，并在网络间传输。
///
/// # 相等性
///
/// 实现了 PartialEq trait，可以比较两个日志条目是否相等。
/// 这对于日志一致性检查非常重要。
///
/// # 使用示例
///
/// ```rust
/// use distributed_kv_store::storage::log::{LogEntry, LogEntryType};
///
/// // 创建应用数据条目
/// let app_entry = LogEntry {
///     term: 2,
///     index: 10,
///     data: serde_json::to_vec(&"PUT key value").unwrap(),
///     entry_type: LogEntryType::Application,
/// };
///
/// // 创建配置变更条目
/// let config_entry = LogEntry {
///     term: 2,
///     index: 11,
///     data: serde_json::to_vec(&"ADD_NODE node3").unwrap(),
///     entry_type: LogEntryType::Configuration,
/// };
///
/// assert_eq!(app_entry.term, 2);
/// assert_eq!(config_entry.index, 11);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogEntry {
    pub term: Term,
    pub index: LogIndex,
    pub data: Vec<u8>,
    pub entry_type: LogEntryType,
}

/// Types of log entries
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LogEntryType {
    /// Normal application data
    Application,
    /// Configuration change
    Configuration,
    /// No-op entry (used by leaders)
    NoOp,
}

/// Log storage manager
pub struct LogStorage {
    tree: Tree,
}

impl LogStorage {
    /// Create a new log storage instance
    pub fn new(tree: Tree) -> Self {
        Self { tree }
    }
    
    /// Append a single log entry
    pub fn append_entry(&self, entry: LogEntry) -> StorageResult<()> {
        let key = Self::index_to_key(entry.index);
        let data = bincode::serialize(&entry)?;
        self.tree.insert(key, data)?;
        Ok(())
    }
    
    /// Append multiple log entries
    pub fn append_entries(&self, entries: Vec<LogEntry>) -> StorageResult<()> {
        let mut batch = sled::Batch::default();
        
        for entry in entries {
            let key = Self::index_to_key(entry.index);
            let data = bincode::serialize(&entry)?;
            batch.insert(key, data);
        }
        
        self.tree.apply_batch(batch)?;
        Ok(())
    }
    
    /// Get a log entry by index
    pub fn get_entry(&self, index: LogIndex) -> StorageResult<Option<LogEntry>> {
        let key = Self::index_to_key(index);
        match self.tree.get(key)? {
            Some(data) => {
                let entry: LogEntry = bincode::deserialize(&data)?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }
    
    /// Get multiple log entries in range [start, end)
    pub fn get_entries(&self, start: LogIndex, end: LogIndex) -> StorageResult<Vec<LogEntry>> {
        let mut entries = Vec::new();
        
        for index in start..end {
            if let Some(entry) = self.get_entry(index)? {
                entries.push(entry);
            } else {
                break; // Stop at first missing entry
            }
        }
        
        Ok(entries)
    }
    
    /// Get entries starting from index with limit
    pub fn get_entries_from(&self, start: LogIndex, limit: usize) -> StorageResult<Vec<LogEntry>> {
        let mut entries = Vec::new();
        let mut index = start;
        
        while entries.len() < limit {
            if let Some(entry) = self.get_entry(index)? {
                entries.push(entry);
                index += 1;
            } else {
                break;
            }
        }
        
        Ok(entries)
    }
    
    /// Delete log entries from index onwards
    pub fn delete_from(&self, from_index: LogIndex) -> StorageResult<()> {
        let start_key = Self::index_to_key(from_index);
        
        // Find all keys to delete
        let keys_to_delete: Vec<_> = self.tree
            .range(start_key..)
            .keys()
            .collect::<Result<Vec<_>, _>>()?;
        
        // Delete in batch
        let mut batch = sled::Batch::default();
        for key in keys_to_delete {
            batch.remove(key);
        }
        
        self.tree.apply_batch(batch)?;
        Ok(())
    }
    
    /// Get the last log index
    pub fn last_index(&self) -> StorageResult<LogIndex> {
        match self.tree.last()? {
            Some((key, _)) => {
                let index = Self::key_to_index(&key)?;
                Ok(index)
            }
            None => Ok(0), // No entries, return 0
        }
    }
    
    /// Get the first log index
    pub fn first_index(&self) -> StorageResult<LogIndex> {
        match self.tree.first()? {
            Some((key, _)) => {
                let index = Self::key_to_index(&key)?;
                Ok(index)
            }
            None => Ok(1), // No entries, return 1 (Raft log is 1-indexed)
        }
    }
    
    /// Get the term of the last log entry
    pub fn last_term(&self) -> StorageResult<Term> {
        let last_index = self.last_index()?;
        if last_index == 0 {
            return Ok(0);
        }
        
        match self.get_entry(last_index)? {
            Some(entry) => Ok(entry.term),
            None => Ok(0),
        }
    }
    
    /// Get log statistics
    pub fn stats(&self) -> StorageResult<LogStats> {
        let first_index = self.first_index()?;
        let last_index = self.last_index()?;
        let entry_count = if last_index >= first_index {
            last_index - first_index + 1
        } else {
            0
        };
        
        Ok(LogStats {
            first_index,
            last_index,
            entry_count,
        })
    }
    
    /// Compact log by removing entries before the given index
    pub fn compact(&self, before_index: LogIndex) -> StorageResult<()> {
        let end_key = Self::index_to_key(before_index);
        
        // Find all keys to delete
        let keys_to_delete: Vec<_> = self.tree
            .range(..end_key)
            .keys()
            .collect::<Result<Vec<_>, _>>()?;
        
        // Delete in batch
        let mut batch = sled::Batch::default();
        for key in keys_to_delete {
            batch.remove(key);
        }
        
        self.tree.apply_batch(batch)?;
        Ok(())
    }
    
    /// Convert log index to storage key
    fn index_to_key(index: LogIndex) -> Vec<u8> {
        index.to_be_bytes().to_vec()
    }
    
    /// Convert storage key to log index
    fn key_to_index(key: &[u8]) -> StorageResult<LogIndex> {
        if key.len() != 8 {
            return Err(StorageError::InvalidFormat);
        }
        
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(key);
        Ok(LogIndex::from_be_bytes(bytes))
    }
}

/// Log storage statistics
#[derive(Debug, Clone)]
pub struct LogStats {
    pub first_index: LogIndex,
    pub last_index: LogIndex,
    pub entry_count: LogIndex,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    fn create_test_log_storage() -> (LogStorage, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db = sled::open(temp_dir.path()).unwrap();
        let tree = db.open_tree("test_log").unwrap();
        let log_storage = LogStorage::new(tree);
        (log_storage, temp_dir)
    }
    
    fn create_test_entry(index: LogIndex, term: Term, data: &str) -> LogEntry {
        LogEntry {
            term,
            index,
            data: data.as_bytes().to_vec(),
            entry_type: LogEntryType::Application,
        }
    }
    
    #[test]
    fn test_append_and_get_entry() {
        let (log_storage, _temp_dir) = create_test_log_storage();
        
        let entry = create_test_entry(1, 1, "test data");
        log_storage.append_entry(entry.clone()).unwrap();
        
        let retrieved = log_storage.get_entry(1).unwrap().unwrap();
        assert_eq!(retrieved, entry);
        
        // Test non-existent entry
        let non_existent = log_storage.get_entry(2).unwrap();
        assert!(non_existent.is_none());
    }
    
    #[test]
    fn test_append_multiple_entries() {
        let (log_storage, _temp_dir) = create_test_log_storage();
        
        let entries = vec![
            create_test_entry(1, 1, "data1"),
            create_test_entry(2, 1, "data2"),
            create_test_entry(3, 2, "data3"),
        ];
        
        log_storage.append_entries(entries.clone()).unwrap();
        
        for (i, expected) in entries.iter().enumerate() {
            let retrieved = log_storage.get_entry((i + 1) as LogIndex).unwrap().unwrap();
            assert_eq!(retrieved, *expected);
        }
    }
    
    #[test]
    fn test_get_entries_range() {
        let (log_storage, _temp_dir) = create_test_log_storage();
        
        let entries = vec![
            create_test_entry(1, 1, "data1"),
            create_test_entry(2, 1, "data2"),
            create_test_entry(3, 2, "data3"),
            create_test_entry(4, 2, "data4"),
        ];
        
        log_storage.append_entries(entries.clone()).unwrap();
        
        let retrieved = log_storage.get_entries(2, 4).unwrap();
        assert_eq!(retrieved.len(), 2);
        assert_eq!(retrieved[0], entries[1]);
        assert_eq!(retrieved[1], entries[2]);
    }
    
    #[test]
    fn test_delete_from() {
        let (log_storage, _temp_dir) = create_test_log_storage();
        
        let entries = vec![
            create_test_entry(1, 1, "data1"),
            create_test_entry(2, 1, "data2"),
            create_test_entry(3, 2, "data3"),
            create_test_entry(4, 2, "data4"),
        ];
        
        log_storage.append_entries(entries).unwrap();
        
        // Delete from index 3 onwards
        log_storage.delete_from(3).unwrap();
        
        // Check that entries 1 and 2 still exist
        assert!(log_storage.get_entry(1).unwrap().is_some());
        assert!(log_storage.get_entry(2).unwrap().is_some());
        
        // Check that entries 3 and 4 are deleted
        assert!(log_storage.get_entry(3).unwrap().is_none());
        assert!(log_storage.get_entry(4).unwrap().is_none());
    }
    
    #[test]
    fn test_last_index_and_term() {
        let (log_storage, _temp_dir) = create_test_log_storage();
        
        // Empty log
        assert_eq!(log_storage.last_index().unwrap(), 0);
        assert_eq!(log_storage.last_term().unwrap(), 0);
        
        // Add entries
        let entries = vec![
            create_test_entry(1, 1, "data1"),
            create_test_entry(2, 2, "data2"),
        ];
        
        log_storage.append_entries(entries).unwrap();
        
        assert_eq!(log_storage.last_index().unwrap(), 2);
        assert_eq!(log_storage.last_term().unwrap(), 2);
    }
    
    #[test]
    fn test_stats() {
        let (log_storage, _temp_dir) = create_test_log_storage();
        
        let entries = vec![
            create_test_entry(5, 1, "data1"),
            create_test_entry(6, 1, "data2"),
            create_test_entry(7, 2, "data3"),
        ];
        
        log_storage.append_entries(entries).unwrap();
        
        let stats = log_storage.stats().unwrap();
        assert_eq!(stats.first_index, 5);
        assert_eq!(stats.last_index, 7);
        assert_eq!(stats.entry_count, 3);
    }
}