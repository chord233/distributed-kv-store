//! # Raft 节点实现模块
//!
//! 本模块定义了 Raft 集群中单个节点的表示和管理。每个 RaftNode 包含了
//! 节点的状态信息、网络地址、日志索引等关键数据，用于 Raft 算法的
//! 领导者选举和日志复制过程。
//!
//! ## 功能特性
//!
//! - **节点状态管理**: 跟踪节点的当前状态（Follower/Candidate/Leader）
//! - **心跳监控**: 监控节点的活跃状态和网络连通性
//! - **日志索引跟踪**: 管理日志复制的进度信息
//! - **网络地址管理**: 存储节点的网络连接信息
//!
//! ## 核心结构
//!
//! - [`RaftNode`]: 表示集群中的单个节点
//!
//! ## 使用示例
//!
//! ```rust
//! use distributed_kv_store::raft::node::RaftNode;
//!
//! // 创建新节点
//! let node = RaftNode::new(1, "127.0.0.1:8080".to_string());
//! println!("节点 {} 地址: {}", node.node_id, node.address);
//!
//! // 更新心跳时间
//! let mut node = node;
//! node.update_heartbeat();
//! ```

use super::{RaftError, RaftResult, RaftState};

use crate::{LogIndex, NodeId, Term};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Raft 集群中的节点表示
///
/// 每个 RaftNode 实例代表 Raft 集群中的一个节点，包含了该节点的
/// 所有状态信息和元数据。这些信息用于 Raft 算法的各个阶段，
/// 包括领导者选举、日志复制和故障检测。
///
/// # 字段说明
///
/// - `node_id`: 节点的唯一标识符
/// - `address`: 节点的网络地址，用于建立连接
/// - `state`: 节点当前的 Raft 状态
/// - `last_term`: 节点已知的最新任期
/// - `last_heartbeat`: 最后一次收到心跳的时间
/// - `next_index`: 下一个要发送的日志索引（仅领导者使用）
/// - `match_index`: 已确认复制的最高日志索引（仅领导者使用）
/// - `is_reachable`: 节点是否可达
///
/// # 线程安全
///
/// RaftNode 实现了 Clone trait，可以安全地在多个线程间共享。
/// 但需要注意，修改操作需要适当的同步机制。
///
/// # 使用示例
///
/// ```rust
/// use distributed_kv_store::raft::node::RaftNode;
/// use std::time::Instant;
///
/// let mut node = RaftNode::new(1, "192.168.1.100:8080".to_string());
///
/// // 检查节点状态
/// println!("节点 {} 当前状态: {:?}", node.node_id, node.state);
///
/// // 更新心跳时间
/// node.update_heartbeat();
///
/// // 检查节点是否超时
/// let timeout = std::time::Duration::from_secs(5);
/// if node.is_heartbeat_timeout(timeout) {
///     println!("节点 {} 心跳超时", node.node_id);
/// }
/// ```
#[derive(Debug, Clone)]
pub struct RaftNode {
    /// Node identifier
    pub node_id: NodeId,
    /// Node address
    pub address: String,
    /// Current state
    pub state: RaftState,
    /// Last known term
    pub last_term: Term,
    /// Last heartbeat time
    pub last_heartbeat: Instant,
    /// Next log index to send (leader only)
    pub next_index: LogIndex,
    /// Highest log index replicated (leader only)
    pub match_index: LogIndex,
    /// Whether the node is reachable
    pub is_reachable: bool,
}

impl RaftNode {
    /// Create a new Raft node
    pub fn new(node_id: NodeId, address: String) -> Self {
        Self {
            node_id,
            address,
            state: RaftState::Follower,
            last_term: 0,
            last_heartbeat: Instant::now(),
            next_index: 1,
            match_index: 0,
            is_reachable: true,
        }
    }
    
    /// Update last heartbeat time
    pub fn update_heartbeat(&mut self) {
        self.last_heartbeat = Instant::now();
        self.is_reachable = true;
    }
    
    /// Check if the node is considered dead (no heartbeat for too long)
    pub fn is_dead(&self, timeout: Duration) -> bool {
        self.last_heartbeat.elapsed() > timeout
    }
    
    /// Update the node's state
    pub fn update_state(&mut self, state: RaftState, term: Term) {
        self.state = state;
        self.last_term = term;
        self.update_heartbeat();
    }
    
    /// Update next index for log replication
    pub fn update_next_index(&mut self, index: LogIndex) {
        self.next_index = index;
    }
    
    /// Update match index for log replication
    pub fn update_match_index(&mut self, index: LogIndex) {
        self.match_index = index;
        if self.next_index <= index {
            self.next_index = index + 1;
        }
    }
    
    /// Mark node as unreachable
    pub fn mark_unreachable(&mut self) {
        self.is_reachable = false;
    }
}

/// Node manager for tracking cluster peers
pub struct NodeManager {
    /// Current node ID
    node_id: NodeId,
    /// Map of peer nodes
    nodes: RwLock<HashMap<NodeId, RaftNode>>,
    /// Heartbeat timeout
    heartbeat_timeout: Duration,
}

impl NodeManager {
    /// Create a new node manager
    pub fn new(node_id: NodeId, heartbeat_timeout: Duration) -> Self {
        Self {
            node_id,
            nodes: RwLock::new(HashMap::new()),
            heartbeat_timeout,
        }
    }
    
    /// Add a new peer node
    pub async fn add_node(&self, node_id: NodeId, address: String) -> RaftResult<()> {
        if node_id == self.node_id {
            return Err(RaftError::InvalidStateTransition);
        }
        
        let mut nodes = self.nodes.write().await;
        let node = RaftNode::new(node_id.clone(), address);
        let node_id_for_log = node_id.clone();
        nodes.insert(node_id, node);
        
        info!("Added peer node: {}", node_id_for_log);
        Ok(())
    }
    
    /// Remove a peer node
    pub async fn remove_node(&self, node_id: &NodeId) -> RaftResult<()> {
        let mut nodes = self.nodes.write().await;
        if nodes.remove(node_id).is_some() {
            info!("Removed peer node: {}", node_id);
            Ok(())
        } else {
            Err(RaftError::NodeNotFound(node_id.clone()))
        }
    }
    
    /// Get a peer node
    pub async fn get_node(&self, node_id: &NodeId) -> Option<RaftNode> {
        let nodes = self.nodes.read().await;
        nodes.get(node_id).cloned()
    }
    
    /// Get all peer nodes
    pub async fn get_all_nodes(&self) -> HashMap<NodeId, RaftNode> {
        self.nodes.read().await.clone()
    }
    
    /// Get reachable peer nodes
    pub async fn get_reachable_nodes(&self) -> HashMap<NodeId, RaftNode> {
        let nodes = self.nodes.read().await;
        nodes
            .iter()
            .filter(|(_, node)| node.is_reachable && !node.is_dead(self.heartbeat_timeout))
            .map(|(id, node)| (id.clone(), node.clone()))
            .collect()
    }
    
    /// Update node heartbeat
    pub async fn update_node_heartbeat(&self, node_id: &NodeId) {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            node.update_heartbeat();
        }
    }
    
    /// Update node state
    pub async fn update_node_state(&self, node_id: &NodeId, state: RaftState, term: Term) {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            node.update_state(state, term);
        }
    }
    
    /// Update node next index
    pub async fn update_node_next_index(&self, node_id: &NodeId, index: LogIndex) {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            node.update_next_index(index);
        }
    }
    
    /// Update node match index
    pub async fn update_node_match_index(&self, node_id: &NodeId, index: LogIndex) {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            node.update_match_index(index);
        }
    }
    
    /// Mark node as unreachable
    pub async fn mark_node_unreachable(&self, node_id: &NodeId) {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            node.mark_unreachable();
            warn!("Marked node {} as unreachable", node_id);
        }
    }
    
    /// Get cluster size (including self)
    pub async fn cluster_size(&self) -> usize {
        self.nodes.read().await.len() + 1
    }
    
    /// Get majority threshold
    pub async fn majority_threshold(&self) -> usize {
        let cluster_size = self.cluster_size().await;
        cluster_size / 2 + 1
    }
    
    /// Check if we have majority of reachable nodes
    pub async fn has_majority(&self) -> bool {
        let reachable_count = self.get_reachable_nodes().await.len() + 1; // +1 for self
        let majority = self.majority_threshold().await;
        reachable_count >= majority
    }
    
    /// Get dead nodes (nodes that haven't sent heartbeat recently)
    pub async fn get_dead_nodes(&self) -> Vec<NodeId> {
        let nodes = self.nodes.read().await;
        nodes
            .iter()
            .filter(|(_, node)| node.is_dead(self.heartbeat_timeout))
            .map(|(id, _)| id.clone())
            .collect()
    }
    
    /// Cleanup dead nodes
    pub async fn cleanup_dead_nodes(&self) -> Vec<NodeId> {
        let dead_nodes = self.get_dead_nodes().await;
        let mut nodes = self.nodes.write().await;
        
        for node_id in &dead_nodes {
            nodes.remove(node_id);
            warn!("Removed dead node: {}", node_id);
        }
        
        dead_nodes
    }
    
    /// Get node statistics
    pub async fn get_stats(&self) -> NodeStats {
        let nodes = self.nodes.read().await;
        let total_nodes = nodes.len();
        let reachable_nodes = nodes
            .values()
            .filter(|node| node.is_reachable && !node.is_dead(self.heartbeat_timeout))
            .count();
        let dead_nodes = nodes
            .values()
            .filter(|node| node.is_dead(self.heartbeat_timeout))
            .count();
        
        NodeStats {
            total_nodes,
            reachable_nodes,
            dead_nodes,
            cluster_size: total_nodes + 1, // +1 for self
            majority_threshold: (total_nodes + 1) / 2 + 1,
        }
    }
}

/// Node statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStats {
    pub total_nodes: usize,
    pub reachable_nodes: usize,
    pub dead_nodes: usize,
    pub cluster_size: usize,
    pub majority_threshold: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    
    #[test]
    fn test_raft_node_creation() {
        let node = RaftNode::new("node1".to_string(), "127.0.0.1:8080".to_string());
        
        assert_eq!(node.node_id, "node1");
        assert_eq!(node.address, "127.0.0.1:8080");
        assert_eq!(node.state, RaftState::Follower);
        assert_eq!(node.last_term, 0);
        assert_eq!(node.next_index, 1);
        assert_eq!(node.match_index, 0);
        assert!(node.is_reachable);
    }
    
    #[test]
    fn test_node_heartbeat_update() {
        let mut node = RaftNode::new("node1".to_string(), "127.0.0.1:8080".to_string());
        let initial_time = node.last_heartbeat;
        
        // Wait a bit and update heartbeat
        std::thread::sleep(Duration::from_millis(10));
        node.update_heartbeat();
        
        assert!(node.last_heartbeat > initial_time);
        assert!(node.is_reachable);
    }
    
    #[test]
    fn test_node_death_detection() {
        let mut node = RaftNode::new("node1".to_string(), "127.0.0.1:8080".to_string());
        
        // Node should not be dead immediately
        assert!(!node.is_dead(Duration::from_millis(100)));
        
        // Simulate old heartbeat
        node.last_heartbeat = Instant::now() - Duration::from_millis(200);
        
        // Node should be considered dead now
        assert!(node.is_dead(Duration::from_millis(100)));
    }
    
    #[tokio::test]
    async fn test_node_manager() {
        let manager = NodeManager::new(
            "self".to_string(),
            Duration::from_millis(1000),
        );
        
        // Add nodes
        manager.add_node("node1".to_string(), "127.0.0.1:8080".to_string()).await.unwrap();
        manager.add_node("node2".to_string(), "127.0.0.1:8081".to_string()).await.unwrap();
        
        // Check cluster size
        assert_eq!(manager.cluster_size().await, 3); // 2 peers + self
        assert_eq!(manager.majority_threshold().await, 2);
        
        // Check nodes
        let nodes = manager.get_all_nodes().await;
        assert_eq!(nodes.len(), 2);
        assert!(nodes.contains_key("node1"));
        assert!(nodes.contains_key("node2"));
        
        // Remove node
        manager.remove_node(&"node1".to_string()).await.unwrap();
        assert_eq!(manager.cluster_size().await, 2);
    }
    
    #[tokio::test]
    async fn test_node_state_updates() {
        let manager = NodeManager::new(
            "self".to_string(),
            Duration::from_millis(1000),
        );
        
        manager.add_node("node1".to_string(), "127.0.0.1:8080".to_string()).await.unwrap();
        
        // Update node state
        manager.update_node_state(&"node1".to_string(), RaftState::Leader, 5).await;
        
        let node = manager.get_node(&"node1".to_string()).await.unwrap();
        assert_eq!(node.state, RaftState::Leader);
        assert_eq!(node.last_term, 5);
    }
    
    #[tokio::test]
    async fn test_node_index_updates() {
        let manager = NodeManager::new(
            "self".to_string(),
            Duration::from_millis(1000),
        );
        
        manager.add_node("node1".to_string(), "127.0.0.1:8080".to_string()).await.unwrap();
        
        // Update indices
        manager.update_node_next_index(&"node1".to_string(), 10).await;
        manager.update_node_match_index(&"node1".to_string(), 8).await;
        
        let node = manager.get_node(&"node1".to_string()).await.unwrap();
        assert_eq!(node.next_index, 10);
        assert_eq!(node.match_index, 8);
    }
    
    #[tokio::test]
    async fn test_majority_check() {
        let manager = NodeManager::new(
            "self".to_string(),
            Duration::from_millis(1000),
        );
        
        // Single node cluster - should have majority
        assert!(manager.has_majority().await);
        
        // Add two more nodes
        manager.add_node("node1".to_string(), "127.0.0.1:8080".to_string()).await.unwrap();
        manager.add_node("node2".to_string(), "127.0.0.1:8081".to_string()).await.unwrap();
        
        // 3-node cluster, all reachable - should have majority
        assert!(manager.has_majority().await);
        
        // Mark one node as unreachable
        manager.mark_node_unreachable(&"node1".to_string()).await;
        
        // Still should have majority (2 out of 3)
        assert!(manager.has_majority().await);
        
        // Mark another node as unreachable
        manager.mark_node_unreachable(&"node2".to_string()).await;
        
        // Now should not have majority (1 out of 3)
        assert!(!manager.has_majority().await);
    }
}