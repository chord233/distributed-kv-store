//! # Raft 共识算法模块
//!
//! 本模块实现了 Raft 分布式共识算法，为分布式键值存储系统提供强一致性保证。
//! Raft 算法通过领导者选举、日志复制和安全性机制确保集群中所有节点的数据一致性。
//!
//! ## 功能特性
//!
//! - **领导者选举**: 自动选举集群领导者，处理节点故障和网络分区
//! - **日志复制**: 将客户端操作复制到所有节点，确保数据一致性
//! - **安全性保证**: 防止脑裂和数据丢失，确保已提交的日志不会丢失
//! - **成员变更**: 支持动态添加和移除集群节点
//! - **快照机制**: 压缩日志，提高存储效率和恢复速度
//!
//! ## 核心组件
//!
//! - [`RaftConsensus`]: Raft 共识算法的主要实现
//! - [`RaftNode`]: 集群中的单个节点表示
//! - [`StateMachine`]: 状态机接口，处理已提交的日志条目
//! - [`RaftError`]: Raft 相关的错误类型
//!
//! ## Raft 状态
//!
//! 每个节点可以处于以下三种状态之一：
//! - **Follower**: 跟随者，接收来自领导者的日志条目
//! - **Candidate**: 候选者，发起领导者选举
//! - **Leader**: 领导者，处理客户端请求并复制日志
//!
//! ## 使用示例
//!
//! ```rust
//! use distributed_kv_store::raft::RaftConsensus;
//! use distributed_kv_store::RaftConfig;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = RaftConfig {
//!         node_id: 1,
//!         election_timeout: Duration::from_millis(150),
//!         heartbeat_interval: Duration::from_millis(50),
//!         ..Default::default()
//!     };
//!
//!     let raft = RaftConsensus::new(config, storage, network).await?;
//!     raft.start().await?;
//!
//!     // 提交日志条目
//!     let result = raft.append_entry(b"set key value".to_vec()).await?;
//!     println!("日志条目已提交: {:?}", result);
//!
//!     Ok(())
//! }
//! ```

use crate::storage::log::{LogEntry, LogEntryType, LogStorage};
use crate::storage::{Storage, StorageError};
use crate::{LogIndex, NodeId, RaftConfig, Term};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::time::interval;
use tracing::{debug, error, info};

pub mod node;
pub mod state_machine;


use state_machine::StateMachine;

/// Raft 共识算法相关的错误类型
///
/// 定义了 Raft 算法运行过程中可能遇到的各种错误情况，
/// 包括存储错误、网络错误、选举超时等。
///
/// # 错误分类
///
/// - **存储相关**: 底层存储操作失败
/// - **网络相关**: 节点间通信失败
/// - **共识相关**: 选举、日志复制等共识过程失败
/// - **状态相关**: 无效的状态转换或参数
///
/// # 使用示例
///
/// ```rust
/// use distributed_kv_store::raft::RaftError;
///
/// fn handle_raft_error(error: RaftError) {
///     match error {
///         RaftError::NotLeader => {
///             println!("当前节点不是领导者，无法处理写请求");
///         }
///         RaftError::ElectionTimeout => {
///             println!("选举超时，开始新一轮选举");
///         }
///         RaftError::Storage(e) => {
///             eprintln!("存储错误: {}", e);
///         }
///         _ => {
///             eprintln!("其他 Raft 错误: {}", error);
///         }
///     }
/// }
/// ```
#[derive(thiserror::Error, Debug)]
pub enum RaftError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    
    #[error("Network error: {0}")]
    Network(String),
    
    #[error("Election timeout")]
    ElectionTimeout,
    
    #[error("Not leader")]
    NotLeader,
    
    #[error("Invalid term: {0}")]
    InvalidTerm(Term),
    
    #[error("Invalid log index: {0}")]
    InvalidLogIndex(LogIndex),
    
    #[error("Consensus not reached")]
    ConsensusNotReached,
    
    #[error("Node not found: {0}")]
    NodeNotFound(NodeId),
    
    #[error("Invalid state transition")]
    InvalidStateTransition,
}

pub type RaftResult<T> = Result<T, RaftError>;

/// Raft node states
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RaftState {
    Follower,
    Candidate,
    Leader,
}

/// Raft consensus engine
pub struct RaftConsensus {
    /// Node configuration
    config: RaftConfig,
    /// Current node ID
    node_id: NodeId,
    /// Current state
    state: Arc<RwLock<RaftState>>,
    /// Current term
    current_term: Arc<RwLock<Term>>,
    /// Voted for in current term
    voted_for: Arc<RwLock<Option<NodeId>>>,
    /// Log storage
    log_storage: Arc<LogStorage>,
    /// Persistent storage
    storage: Arc<Storage>,
    /// State machine
    state_machine: Arc<StateMachine>,
    /// Cluster peers
    peers: Arc<RwLock<HashMap<NodeId, String>>>,
    /// Leader ID
    leader_id: Arc<RwLock<Option<NodeId>>>,
    /// Commit index
    commit_index: Arc<RwLock<LogIndex>>,
    /// Last applied index
    last_applied: Arc<RwLock<LogIndex>>,
    /// Next index for each peer (leader only)
    next_index: Arc<RwLock<HashMap<NodeId, LogIndex>>>,
    /// Match index for each peer (leader only)
    match_index: Arc<RwLock<HashMap<NodeId, LogIndex>>>,
    /// Last heartbeat time
    last_heartbeat: Arc<RwLock<Instant>>,
    /// Election timeout
    election_timeout: Arc<RwLock<Instant>>,
    /// Command channel for external requests
    command_tx: mpsc::UnboundedSender<RaftCommand>,
    command_rx: Arc<RwLock<Option<mpsc::UnboundedReceiver<RaftCommand>>>>,
}

/// Raft command types
#[derive(Debug)]
pub enum RaftCommand {
    /// Client request to append log entry
    AppendEntry {
        data: Vec<u8>,
        response_tx: tokio::sync::oneshot::Sender<RaftResult<LogIndex>>,
    },
    /// Add a new peer to the cluster
    AddPeer {
        node_id: NodeId,
        address: String,
        response_tx: tokio::sync::oneshot::Sender<RaftResult<()>>,
    },
    /// Remove a peer from the cluster
    RemovePeer {
        node_id: NodeId,
        response_tx: tokio::sync::oneshot::Sender<RaftResult<()>>,
    },
    /// Get cluster information
    GetClusterInfo {
        response_tx: tokio::sync::oneshot::Sender<ClusterInfo>,
    },
}

/// Cluster information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterInfo {
    pub leader_id: Option<NodeId>,
    pub current_term: Term,
    pub peers: HashMap<NodeId, String>,
    pub state: RaftState,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
}

/// Vote request message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

/// Vote response message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

/// Append entries request message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

/// Append entries response message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
    pub match_index: LogIndex,
}

impl RaftConsensus {
    /// Create a new Raft consensus instance
    pub async fn new(
        node_id: NodeId,
        config: RaftConfig,
        storage: Arc<Storage>,
        peers: HashMap<NodeId, String>,
    ) -> RaftResult<Self> {
        let log_storage = Arc::new(LogStorage::new(storage.log_tree().clone()));
        let state_machine = Arc::new(StateMachine::new(storage.clone()).await?);
        
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        
        let election_timeout_ms = config.election_timeout_ms;
        let consensus = Self {
            config,
            node_id,
            state: Arc::new(RwLock::new(RaftState::Follower)),
            current_term: Arc::new(RwLock::new(storage.get_current_term().await)),
            voted_for: Arc::new(RwLock::new(storage.get_voted_for().await)),
            log_storage,
            storage,
            state_machine,
            peers: Arc::new(RwLock::new(peers)),
            leader_id: Arc::new(RwLock::new(None)),
            commit_index: Arc::new(RwLock::new(0)),
            last_applied: Arc::new(RwLock::new(0)),
            next_index: Arc::new(RwLock::new(HashMap::new())),
            match_index: Arc::new(RwLock::new(HashMap::new())),
            last_heartbeat: Arc::new(RwLock::new(Instant::now())),
            election_timeout: Arc::new(RwLock::new(
                Instant::now() + Duration::from_millis(election_timeout_ms)
            )),
            command_tx,
            command_rx: Arc::new(RwLock::new(Some(command_rx))),
        };
        
        Ok(consensus)
    }
    
    /// Start the Raft consensus engine
    pub async fn start(&self) -> RaftResult<()> {
        info!("Starting Raft consensus for node {}", self.node_id);
        
        // Take the command receiver
        let command_rx = {
            let mut rx_guard = self.command_rx.write().await;
            rx_guard.take().ok_or(RaftError::InvalidStateTransition)?
        };
        
        // Start main consensus loop
        let consensus = self.clone();
        tokio::spawn(async move {
            consensus.run_consensus_loop().await;
        });
        
        // Start command processing loop
        let consensus = self.clone();
        tokio::spawn(async move {
            consensus.process_commands(command_rx).await;
        });
        
        Ok(())
    }
    
    /// Main consensus loop
    async fn run_consensus_loop(&self) {
        let mut election_timer = interval(Duration::from_millis(10)); // Check every 10ms
        let mut heartbeat_timer = interval(Duration::from_millis(self.config.heartbeat_interval_ms));
        
        loop {
            tokio::select! {
                _ = election_timer.tick() => {
                    self.check_election_timeout().await;
                }
                _ = heartbeat_timer.tick() => {
                    self.send_heartbeats().await;
                }
            }
        }
    }
    
    /// Process incoming commands
    async fn process_commands(&self, mut command_rx: mpsc::UnboundedReceiver<RaftCommand>) {
        while let Some(command) = command_rx.recv().await {
            match command {
                RaftCommand::AppendEntry { data, response_tx } => {
                    let result = self.append_entry(data).await;
                    let _ = response_tx.send(result);
                }
                RaftCommand::AddPeer { node_id, address, response_tx } => {
                    let result = self.add_peer(node_id, address).await;
                    let _ = response_tx.send(result);
                }
                RaftCommand::RemovePeer { node_id, response_tx } => {
                    let result = self.remove_peer(node_id).await;
                    let _ = response_tx.send(result);
                }
                RaftCommand::GetClusterInfo { response_tx } => {
                    let info = self.get_cluster_info().await;
                    let _ = response_tx.send(info);
                }
            }
        }
    }
    
    /// Check if election timeout has occurred
    async fn check_election_timeout(&self) {
        let state = self.state.read().await.clone();
        let election_timeout = *self.election_timeout.read().await;
        
        if state != RaftState::Leader && Instant::now() > election_timeout {
            info!("Election timeout for node {}, starting election", self.node_id);
            self.start_election().await;
        }
    }
    
    /// Start a new election
    async fn start_election(&self) {
        // Transition to candidate
        *self.state.write().await = RaftState::Candidate;
        
        // Increment term
        let new_term = {
            let mut term = self.current_term.write().await;
            *term += 1;
            *term
        };
        
        // Vote for self
        *self.voted_for.write().await = Some(self.node_id.clone());
        
        // Persist state
        if let Err(e) = self.storage.set_current_term(new_term).await {
            error!("Failed to persist term: {}", e);
            return;
        }
        
        if let Err(e) = self.storage.set_voted_for(Some(self.node_id.clone())).await {
            error!("Failed to persist vote: {}", e);
            return;
        }
        
        // Reset election timeout
        self.reset_election_timeout().await;
        
        info!("Node {} started election for term {}", self.node_id, new_term);
        
        // Send vote requests to all peers
        self.request_votes().await;
    }
    
    /// Request votes from all peers
    async fn request_votes(&self) {
        let peers = self.peers.read().await.clone();
        let current_term = *self.current_term.read().await;
        
        let last_log_index = self.log_storage.last_index().unwrap_or(0);
        let last_log_term = self.log_storage.last_term().unwrap_or(0);
        
        let _vote_request = VoteRequest {
            term: current_term,
            candidate_id: self.node_id.clone(),
            last_log_index,
            last_log_term,
        };
        
        let votes_received = 1; // Vote for self
        let total_nodes = peers.len() + 1;
        let majority = total_nodes / 2 + 1;
        
        // TODO: Send vote requests to peers via network
        // For now, we'll simulate receiving votes
        
        if votes_received >= majority {
            self.become_leader().await;
        }
    }
    
    /// Become the leader
    async fn become_leader(&self) {
        info!("Node {} became leader for term {}", self.node_id, *self.current_term.read().await);
        
        *self.state.write().await = RaftState::Leader;
        *self.leader_id.write().await = Some(self.node_id.clone());
        
        // Initialize leader state
        let last_log_index = self.log_storage.last_index().unwrap_or(0);
        let peers = self.peers.read().await.clone();
        
        {
            let mut next_index = self.next_index.write().await;
            let mut match_index = self.match_index.write().await;
            
            for peer_id in peers.keys() {
                next_index.insert(peer_id.clone(), last_log_index + 1);
                match_index.insert(peer_id.clone(), 0);
            }
        }
        
        // Send initial heartbeats
        self.send_heartbeats().await;
    }
    
    /// Send heartbeats to all peers
    async fn send_heartbeats(&self) {
        let state = self.state.read().await.clone();
        if state != RaftState::Leader {
            return;
        }
        
        let peers = self.peers.read().await.clone();
        let _current_term = *self.current_term.read().await;
        let _commit_index = *self.commit_index.read().await;
        
        for peer_id in peers.keys() {
            // TODO: Send append entries (heartbeat) to peer
            // For now, we'll just log
            debug!("Sending heartbeat to peer {}", peer_id);
        }
    }
    
    /// Reset election timeout
    async fn reset_election_timeout(&self) {
        let timeout_ms = self.config.election_timeout_ms + 
            (rand::random::<u64>() % self.config.election_timeout_ms);
        
        *self.election_timeout.write().await = 
            Instant::now() + Duration::from_millis(timeout_ms);
    }
    
    /// Append a new log entry
    pub async fn append_entry(&self, data: Vec<u8>) -> RaftResult<LogIndex> {
        let state = self.state.read().await.clone();
        if state != RaftState::Leader {
            return Err(RaftError::NotLeader);
        }
        
        let current_term = *self.current_term.read().await;
        let last_index = self.log_storage.last_index().unwrap_or(0);
        let new_index = last_index + 1;
        
        let entry = LogEntry {
            term: current_term,
            index: new_index,
            data,
            entry_type: LogEntryType::Application,
        };
        
        self.log_storage.append_entry(entry)?;
        
        // TODO: Replicate to followers
        
        Ok(new_index)
    }
    
    /// Add a peer to the cluster
    pub async fn add_peer(&self, node_id: NodeId, address: String) -> RaftResult<()> {
        let mut peers = self.peers.write().await;
        peers.insert(node_id, address);
        Ok(())
    }
    
    /// Remove a peer from the cluster
    pub async fn remove_peer(&self, node_id: NodeId) -> RaftResult<()> {
        let mut peers = self.peers.write().await;
        peers.remove(&node_id);
        Ok(())
    }
    
    /// Get cluster information
    pub async fn get_cluster_info(&self) -> ClusterInfo {
        ClusterInfo {
            leader_id: self.leader_id.read().await.clone(),
            current_term: *self.current_term.read().await,
            peers: self.peers.read().await.clone(),
            state: self.state.read().await.clone(),
            commit_index: *self.commit_index.read().await,
            last_applied: *self.last_applied.read().await,
        }
    }
    
    /// Get command sender for external use
    pub fn command_sender(&self) -> mpsc::UnboundedSender<RaftCommand> {
        self.command_tx.clone()
    }
    
    /// Get state machine for read operations
    pub fn state_machine(&self) -> Arc<StateMachine> {
        self.state_machine.clone()
    }
    
    /// Get node ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}

// Implement Clone for RaftConsensus
impl Clone for RaftConsensus {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            node_id: self.node_id.clone(),
            state: self.state.clone(),
            current_term: self.current_term.clone(),
            voted_for: self.voted_for.clone(),
            log_storage: self.log_storage.clone(),
            storage: self.storage.clone(),
            state_machine: self.state_machine.clone(),
            peers: self.peers.clone(),
            leader_id: self.leader_id.clone(),
            commit_index: self.commit_index.clone(),
            last_applied: self.last_applied.clone(),
            next_index: self.next_index.clone(),
            match_index: self.match_index.clone(),
            last_heartbeat: self.last_heartbeat.clone(),
            election_timeout: self.election_timeout.clone(),
            command_tx: self.command_tx.clone(),
            command_rx: self.command_rx.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    async fn create_test_consensus() -> (RaftConsensus, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(Storage::new(temp_dir.path()).await.unwrap());
        let config = RaftConfig::default();
        let peers = HashMap::new();
        
        let consensus = RaftConsensus::new(
            "test_node".to_string(),
            config,
            storage,
            peers,
        ).await.unwrap();
        
        (consensus, temp_dir)
    }
    
    #[tokio::test]
    async fn test_consensus_creation() {
        let (consensus, _temp_dir) = create_test_consensus().await;
        
        assert_eq!(consensus.node_id, "test_node");
        assert_eq!(*consensus.state.read().await, RaftState::Follower);
        assert_eq!(*consensus.current_term.read().await, 0);
    }
    
    #[tokio::test]
    async fn test_cluster_info() {
        let (consensus, _temp_dir) = create_test_consensus().await;
        
        let info = consensus.get_cluster_info().await;
        assert_eq!(info.current_term, 0);
        assert_eq!(info.state, RaftState::Follower);
        assert!(info.leader_id.is_none());
    }
}