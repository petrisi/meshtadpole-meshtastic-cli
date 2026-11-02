use prost::Message;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=proto");
    let files: Vec<String> = std::fs::read_dir("proto/meshtastic")
        .expect("proto/meshtastic")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".proto"))
        .map(|n| format!("meshtastic/{n}"))
        .collect();
    let fds = protox::compile(&files, ["proto"]).expect("protox compile");
    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("meshtastic_fds.bin");
    std::fs::write(out, fds.encode_to_vec()).unwrap();
}
