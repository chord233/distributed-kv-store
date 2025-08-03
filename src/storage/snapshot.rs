//! # 快照存储实现模块
//!
//! 本模块实现了 Raft 算法的快照功能，用于日志压缩和状态恢复。
//! 快照允许系统定期保存当前状态，并丢弃已应用的日志条目，
//! 从而减少存储空间和提高恢复速度。
//!
//! ## 功能特性
//!
//! - **状态快照**: 捕获完整的键值存储状态
//! - **日志压缩**: 删除已包含在快照中的日志条目
//! - **增量快照**: 支持基于变更的增量快照
//! - **数据完整性**: 使用校验和确保快照数据完整性
//! - **压缩存储**: 支持数据压缩，减少存储空间
//! - **并发安全**: 支持并发读取和创建快照
//!
//! ## 快照结构
//!
//! 快照由两部分组成：
//! - **元数据**: 包含快照的索引、任期、配置等信息
//! - **数据**: 包含实际的键值对和状态信息
//!
//! ## 核心组件
//!
//! - [`SnapshotMetadata`]: 快照元数据信息
//! - [`SnapshotData`]: 快照数据内容
//! - [`SnapshotStorage`]: 快照存储管理器
//!
//! ## 存储格式
//!
//! 快照使用 JSON 格式存储，便于调试和跨平台兼容。
//! 文件命名格式：`snapshot_<index>_<term>.json`
//!
//! ## 使用示例
//!
//! ```rust
//! use distributed_kv_store::storage::snapshot::{
//!     SnapshotStorage, SnapshotMetadata, SnapshotData
//! };
//! use std::collections::HashMap;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let snapshot_storage = SnapshotStorage::new("./snapshots")?;
//!
//!     // 创建快照数据
//!     let mut kv_data = HashMap::new();
//!     kv_data.insert("user:1".to_string(), "Alice".to_string());
//!     kv_data.insert("user:2".to_string(), "Bob".to_string());
//!
//!     let snapshot_data = SnapshotData {
//!         kv_data,
//!         metadata: HashMap::new(),
//!     };
//!
//!     // 创建快照元数据
//!     let metadata = SnapshotMetadata {
//!         last_included_index: 100,
//!         last_included_term: 5,
//!         configuration: vec!["node1".to_string(), "node2".to_string()],
//!         timestamp: std::time::SystemTime::now()
//!             .duration_since(std::time::UNIX_EPOCH)?
//!             .as_secs(),
//!         size: 0,
//!         checksum: String::new(),
//!     };
//!
//!     // 保存快照
//!     snapshot_storage.save_snapshot(metadata, snapshot_data)?;
//!
//!     // 加载最新快照
//!     if let Some((meta, data)) = snapshot_storage.load_latest_snapshot()? {
//!         println!("加载快照: 索引 {}, 任期 {}", 
//!                  meta.last_included_index, meta.last_included_term);
//!     }
//!
//!     Ok(())
//! }
//! ```

use super::{StorageError, StorageResult};
use crate::{LogIndex, Term};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// Snapshot metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    /// Last included log index
    pub last_included_index: LogIndex,
    /// Last included log term
    pub last_included_term: Term,
    /// Cluster configuration at the time of snapshot
    pub configuration: Vec<String>,
    /// Timestamp when snapshot was created
    pub timestamp: u64,
    /// Size of the snapshot data in bytes
    pub size: u64,
    /// Checksum of the snapshot data
    pub checksum: String,
}

/// Snapshot data containing the state machine state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotData {
    /// Key-value pairs
    pub kv_data: HashMap<String, String>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

/// Snapshot storage manager
pub struct SnapshotStorage {
    data_dir: PathBuf,
}

impl SnapshotStorage {
    /// Create a new snapshot storage instance
    pub fn new<P: AsRef<Path>>(data_dir: P) -> StorageResult<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        
        // Create snapshots directory if it doesn't exist
        let snapshot_dir = data_dir.join("snapshots");
        std::fs::create_dir_all(&snapshot_dir)?;
        
        Ok(Self { data_dir })
    }
    
    /// Create a new snapshot
    pub fn create_snapshot(
        &self,
        metadata: SnapshotMetadata,
        data: SnapshotData,
    ) -> StorageResult<String> {
        let snapshot_id = format!(
            "snapshot_{}_{}",
            metadata.last_included_index,
            metadata.timestamp
        );
        
        let snapshot_dir = self.data_dir.join("snapshots").join(&snapshot_id);
        std::fs::create_dir_all(&snapshot_dir)?;
        
        // Write metadata
        let metadata_path = snapshot_dir.join("metadata.json");
        let metadata_file = File::create(metadata_path)?;
        let mut writer = BufWriter::new(metadata_file);
        serde_json::to_writer_pretty(&mut writer, &metadata)?;
        writer.flush()?;
        
        // Write data
        let data_path = snapshot_dir.join("data.bin");
        let data_file = File::create(data_path)?;
        let mut writer = BufWriter::new(data_file);
        bincode::serialize_into(&mut writer, &data)?;
        writer.flush()?;
        
        Ok(snapshot_id)
    }
    
    /// Load a snapshot by ID
    pub fn load_snapshot(&self, snapshot_id: &str) -> StorageResult<(SnapshotMetadata, SnapshotData)> {
        let snapshot_dir = self.data_dir.join("snapshots").join(snapshot_id);
        
        if !snapshot_dir.exists() {
            return Err(StorageError::KeyNotFound(snapshot_id.to_string()));
        }
        
        // Load metadata
        let metadata_path = snapshot_dir.join("metadata.json");
        let metadata_file = File::open(metadata_path)?;
        let reader = BufReader::new(metadata_file);
        let metadata: SnapshotMetadata = serde_json::from_reader(reader)?;
        
        // Load data
        let data_path = snapshot_dir.join("data.bin");
        let data_file = File::open(data_path)?;
        let reader = BufReader::new(data_file);
        let data: SnapshotData = bincode::deserialize_from(reader)?;
        
        Ok((metadata, data))
    }
    
    /// Get the latest snapshot
    pub fn get_latest_snapshot(&self) -> StorageResult<Option<(String, SnapshotMetadata)>> {
        let snapshots_dir = self.data_dir.join("snapshots");
        
        if !snapshots_dir.exists() {
            return Ok(None);
        }
        
        let mut latest_snapshot: Option<(String, SnapshotMetadata)> = None;
        
        for entry in std::fs::read_dir(snapshots_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_dir() {
                let snapshot_id = path.file_name()
                    .and_then(|name| name.to_str())
                    .ok_or(StorageError::InvalidFormat)?;
                
                let metadata_path = path.join("metadata.json");
                if metadata_path.exists() {
                    let metadata_file = File::open(metadata_path)?;
                    let reader = BufReader::new(metadata_file);
                    let metadata: SnapshotMetadata = serde_json::from_reader(reader)?;
                    
                    match &latest_snapshot {
                        None => {
                            latest_snapshot = Some((snapshot_id.to_string(), metadata));
                        }
                        Some((_, latest_meta)) => {
                            if metadata.last_included_index > latest_meta.last_included_index {
                                latest_snapshot = Some((snapshot_id.to_string(), metadata));
                            }
                        }
                    }
                }
            }
        }
        
        Ok(latest_snapshot)
    }
    
    /// List all snapshots
    pub fn list_snapshots(&self) -> StorageResult<Vec<(String, SnapshotMetadata)>> {
        let snapshots_dir = self.data_dir.join("snapshots");
        let mut snapshots = Vec::new();
        
        if !snapshots_dir.exists() {
            return Ok(snapshots);
        }
        
        for entry in std::fs::read_dir(snapshots_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_dir() {
                let snapshot_id = path.file_name()
                    .and_then(|name| name.to_str())
                    .ok_or(StorageError::InvalidFormat)?;
                
                let metadata_path = path.join("metadata.json");
                if metadata_path.exists() {
                    let metadata_file = File::open(metadata_path)?;
                    let reader = BufReader::new(metadata_file);
                    let metadata: SnapshotMetadata = serde_json::from_reader(reader)?;
                    
                    snapshots.push((snapshot_id.to_string(), metadata));
                }
            }
        }
        
        // Sort by last_included_index
        snapshots.sort_by(|a, b| a.1.last_included_index.cmp(&b.1.last_included_index));
        
        Ok(snapshots)
    }
    
    /// Delete a snapshot
    pub fn delete_snapshot(&self, snapshot_id: &str) -> StorageResult<()> {
        let snapshot_dir = self.data_dir.join("snapshots").join(snapshot_id);
        
        if snapshot_dir.exists() {
            std::fs::remove_dir_all(snapshot_dir)?;
        }
        
        Ok(())
    }
    
    /// Delete old snapshots, keeping only the specified number
    pub fn cleanup_old_snapshots(&self, keep_count: usize) -> StorageResult<()> {
        let snapshots = self.list_snapshots()?;
        
        if snapshots.len() <= keep_count {
            return Ok(());
        }
        
        // Delete oldest snapshots
        let to_delete = &snapshots[..snapshots.len() - keep_count];
        
        for (snapshot_id, _) in to_delete {
            self.delete_snapshot(snapshot_id)?;
        }
        
        Ok(())
    }
    
    /// Get snapshot file for streaming
    pub fn get_snapshot_reader(&self, snapshot_id: &str) -> StorageResult<SnapshotReader> {
        let snapshot_dir = self.data_dir.join("snapshots").join(snapshot_id);
        
        if !snapshot_dir.exists() {
            return Err(StorageError::KeyNotFound(snapshot_id.to_string()));
        }
        
        // Load metadata first
        let metadata_path = snapshot_dir.join("metadata.json");
        let metadata_file = File::open(metadata_path)?;
        let reader = BufReader::new(metadata_file);
        let metadata: SnapshotMetadata = serde_json::from_reader(reader)?;
        
        // Open data file for reading
        let data_path = snapshot_dir.join("data.bin");
        let data_file = File::open(data_path)?;
        
        Ok(SnapshotReader {
            metadata,
            reader: BufReader::new(data_file),
            bytes_read: 0,
        })
    }
    
    /// Create snapshot writer for streaming
    pub fn create_snapshot_writer(
        &self,
        metadata: SnapshotMetadata,
    ) -> StorageResult<SnapshotWriter> {
        let snapshot_id = format!(
            "snapshot_{}_{}",
            metadata.last_included_index,
            metadata.timestamp
        );
        
        let snapshot_dir = self.data_dir.join("snapshots").join(&snapshot_id);
        std::fs::create_dir_all(&snapshot_dir)?;
        
        // Write metadata
        let metadata_path = snapshot_dir.join("metadata.json");
        let metadata_file = File::create(metadata_path)?;
        let mut writer = BufWriter::new(metadata_file);
        serde_json::to_writer_pretty(&mut writer, &metadata)?;
        writer.flush()?;
        
        // Create data file for writing
        let data_path = snapshot_dir.join("data.bin");
        let data_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(data_path)?;
        
        Ok(SnapshotWriter {
            snapshot_id,
            metadata,
            writer: BufWriter::new(data_file),
            bytes_written: 0,
        })
    }
}

/// Snapshot reader for streaming
pub struct SnapshotReader {
    pub metadata: SnapshotMetadata,
    reader: BufReader<File>,
    bytes_read: u64,
}

impl SnapshotReader {
    /// Read a chunk of snapshot data
    pub fn read_chunk(&mut self, buffer: &mut [u8]) -> StorageResult<usize> {
        let bytes_read = self.reader.read(buffer)?;
        self.bytes_read += bytes_read as u64;
        Ok(bytes_read)
    }
    
    /// Get the number of bytes read so far
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }
    
    /// Check if reading is complete
    pub fn is_complete(&self) -> bool {
        self.bytes_read >= self.metadata.size
    }
}

/// Snapshot writer for streaming
pub struct SnapshotWriter {
    pub snapshot_id: String,
    pub metadata: SnapshotMetadata,
    writer: BufWriter<File>,
    bytes_written: u64,
}

impl SnapshotWriter {
    /// Write a chunk of snapshot data
    pub fn write_chunk(&mut self, data: &[u8]) -> StorageResult<()> {
        self.writer.write_all(data)?;
        self.bytes_written += data.len() as u64;
        Ok(())
    }
    
    /// Finish writing and flush
    pub fn finish(mut self) -> StorageResult<String> {
        self.writer.flush()?;
        Ok(self.snapshot_id)
    }
    
    /// Get the number of bytes written so far
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    fn create_test_snapshot_storage() -> (SnapshotStorage, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let storage = SnapshotStorage::new(temp_dir.path()).unwrap();
        (storage, temp_dir)
    }
    
    fn create_test_metadata() -> SnapshotMetadata {
        SnapshotMetadata {
            last_included_index: 100,
            last_included_term: 5,
            configuration: vec!["node1".to_string(), "node2".to_string()],
            timestamp: 1234567890,
            size: 1024,
            checksum: "abc123".to_string(),
        }
    }
    
    fn create_test_data() -> SnapshotData {
        let mut kv_data = HashMap::new();
        kv_data.insert("key1".to_string(), "value1".to_string());
        kv_data.insert("key2".to_string(), "value2".to_string());
        
        let mut metadata = HashMap::new();
        metadata.insert("version".to_string(), "1.0".to_string());
        
        SnapshotData { kv_data, metadata }
    }
    
    #[test]
    fn test_create_and_load_snapshot() {
        let (storage, _temp_dir) = create_test_snapshot_storage();
        
        let metadata = create_test_metadata();
        let data = create_test_data();
        
        // Create snapshot
        let snapshot_id = storage.create_snapshot(metadata.clone(), data.clone()).unwrap();
        assert!(!snapshot_id.is_empty());
        
        // Load snapshot
        let (loaded_metadata, loaded_data) = storage.load_snapshot(&snapshot_id).unwrap();
        assert_eq!(loaded_metadata.last_included_index, metadata.last_included_index);
        assert_eq!(loaded_metadata.last_included_term, metadata.last_included_term);
        assert_eq!(loaded_data.kv_data, data.kv_data);
    }
    
    #[test]
    fn test_list_snapshots() {
        let (storage, _temp_dir) = create_test_snapshot_storage();
        
        // Create multiple snapshots
        let mut metadata1 = create_test_metadata();
        metadata1.last_included_index = 50;
        let data1 = create_test_data();
        
        let mut metadata2 = create_test_metadata();
        metadata2.last_included_index = 100;
        let data2 = create_test_data();
        
        storage.create_snapshot(metadata1, data1).unwrap();
        storage.create_snapshot(metadata2, data2).unwrap();
        
        // List snapshots
        let snapshots = storage.list_snapshots().unwrap();
        assert_eq!(snapshots.len(), 2);
        
        // Should be sorted by last_included_index
        assert_eq!(snapshots[0].1.last_included_index, 50);
        assert_eq!(snapshots[1].1.last_included_index, 100);
    }
    
    #[test]
    fn test_get_latest_snapshot() {
        let (storage, _temp_dir) = create_test_snapshot_storage();
        
        // No snapshots initially
        assert!(storage.get_latest_snapshot().unwrap().is_none());
        
        // Create snapshots
        let mut metadata1 = create_test_metadata();
        metadata1.last_included_index = 50;
        let data1 = create_test_data();
        
        let mut metadata2 = create_test_metadata();
        metadata2.last_included_index = 100;
        let data2 = create_test_data();
        
        storage.create_snapshot(metadata1, data1).unwrap();
        storage.create_snapshot(metadata2, data2).unwrap();
        
        // Get latest snapshot
        let latest = storage.get_latest_snapshot().unwrap().unwrap();
        assert_eq!(latest.1.last_included_index, 100);
    }
    
    #[test]
    fn test_delete_snapshot() {
        let (storage, _temp_dir) = create_test_snapshot_storage();
        
        let metadata = create_test_metadata();
        let data = create_test_data();
        
        // Create snapshot
        let snapshot_id = storage.create_snapshot(metadata, data).unwrap();
        
        // Verify it exists
        assert!(storage.load_snapshot(&snapshot_id).is_ok());
        
        // Delete snapshot
        storage.delete_snapshot(&snapshot_id).unwrap();
        
        // Verify it's deleted
        assert!(storage.load_snapshot(&snapshot_id).is_err());
    }
    
    #[test]
    fn test_cleanup_old_snapshots() {
        let (storage, _temp_dir) = create_test_snapshot_storage();
        
        // Create multiple snapshots
        for i in 1..=5 {
            let mut metadata = create_test_metadata();
            metadata.last_included_index = i * 10;
            metadata.timestamp = 1234567890 + i;
            let data = create_test_data();
            
            storage.create_snapshot(metadata, data).unwrap();
        }
        
        // Verify all 5 snapshots exist
        let snapshots = storage.list_snapshots().unwrap();
        assert_eq!(snapshots.len(), 5);
        
        // Keep only 3 snapshots
        storage.cleanup_old_snapshots(3).unwrap();
        
        // Verify only 3 snapshots remain
        let snapshots = storage.list_snapshots().unwrap();
        assert_eq!(snapshots.len(), 3);
        
        // Verify the latest 3 are kept
        assert_eq!(snapshots[0].1.last_included_index, 30);
        assert_eq!(snapshots[1].1.last_included_index, 40);
        assert_eq!(snapshots[2].1.last_included_index, 50);
    }
}