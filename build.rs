//! Build script for generating protobuf code

use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    
    // Configure tonic-build
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out_dir.join("kvstore_descriptor.bin"))
        .out_dir(&out_dir)
        .compile(
            &[
                "proto/kvstore.proto",
            ],
            &["proto"],
        )?;
    
    // Tell cargo to rerun this build script if the proto files change
    println!("cargo:rerun-if-changed=proto/kvstore.proto");
    
    Ok(())
}