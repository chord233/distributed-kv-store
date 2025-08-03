//! # 分布式键值存储系统 - 主入口
//!
//! 这是分布式键值存储系统的默认主入口文件。
//! 实际的服务器和客户端程序位于 `src/bin/` 目录下：
//!
//! - `kvstore-server`: 服务器程序
//! - `kvstore-client`: 客户端程序
//!
//! ## 使用方法
//!
//! 启动服务器：
//! ```bash
//! cargo run --bin kvstore-server
//! ```
//!
//! 使用客户端：
//! ```bash
//! cargo run --bin kvstore-client -- put mykey myvalue
//! ```

fn main() {
    println!("分布式键值存储系统");
    println!("请使用以下命令启动服务器或客户端：");
    println!("  服务器: cargo run --bin kvstore-server");
    println!("  客户端: cargo run --bin kvstore-client -- --help");
}
