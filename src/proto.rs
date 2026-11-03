//! Runtime protobuf access via descriptors compiled from vendored Meshtastic .proto files.

use anyhow::{Context, Result, anyhow};
use prost_reflect::{DescriptorPool, DynamicMessage, SerializeOptions};
use serde_json::Value;
use std::sync::OnceLock;

static FDS: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/meshtastic_fds.bin"));

pub fn pool() -> &'static DescriptorPool {
    static POOL: OnceLock<DescriptorPool> = OnceLock::new();
    POOL.get_or_init(|| DescriptorPool::decode(FDS).expect("embedded descriptor set"))
}

fn opts() -> SerializeOptions {
    SerializeOptions::new()
        .use_proto_field_name(true)
        .skip_default_fields(true)
        .stringify_64_bit_integers(false)
}

/// Decode `bytes` as `meshtastic.<name>` into proto3-JSON (proto field names, defaults omitted).
pub fn decode_json(name: &str, bytes: &[u8]) -> Result<Value> {
    let full = format!("meshtastic.{name}");
    let desc = pool()
        .get_message_by_name(&full)
        .ok_or_else(|| anyhow!("unknown message {full}"))?;
    let msg = DynamicMessage::decode(desc, bytes).with_context(|| format!("decode {full}"))?;
    Ok(msg.serialize_with_options(serde_json::value::Serializer, &opts())?)
}

/// Encode proto3-JSON into `meshtastic.<name>` wire bytes.
pub fn encode_json(name: &str, v: &Value) -> Result<Vec<u8>> {
    use prost::Message;
    let full = format!("meshtastic.{name}");
    let desc = pool()
        .get_message_by_name(&full)
        .ok_or_else(|| anyhow!("unknown message {full}"))?;
    let msg = DynamicMessage::deserialize(desc, v.clone()).with_context(|| format!("json -> {full}"))?;
    Ok(msg.encode_to_vec())
}

/// PortNum enum value name, if known.
pub fn port_name(n: i32) -> Option<String> {
    pool()
        .get_enum_by_name("meshtastic.PortNum")?
        .get_value(n)
        .map(|v| v.name().to_string())
}

/// Payload message type carried by a port, or how to render it.
pub enum PayloadKind {
    Message(&'static str),
    Text,
    Raw,
}

pub fn payload_kind(portnum: i32) -> PayloadKind {
    use PayloadKind::*;
    match portnum {
        1 | 10 | 11 | 32 | 66 => Text,
        3 => Message("Position"),
        4 => Message("User"),
        5 => Message("Routing"),
        6 => Message("AdminMessage"),
        8 => Message("Waypoint"),
        12 => Message("KeyVerification"),
        34 => Message("Paxcount"),
        35 => Message("StoreForwardPlusPlus"),
        36 => Message("StatusMessage"),
        37 => Message("MeshBeacon"),
        65 => Message("StoreAndForward"),
        67 => Message("Telemetry"),
        70 => Message("RouteDiscovery"),
        71 => Message("NeighborInfo"),
        73 => Message("MapReport"),
        2 => Message("HardwareMessage"),
        _ => Raw,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Request encodings must match firmware expectations exactly (empty sub-message sets the oneof variant).
    #[test]
    fn telemetry_request_bytes() {
        let t = encode_json("Telemetry", &json!({"device_metrics": {}})).unwrap();
        assert_eq!(t, vec![0x12, 0x00]); // field 2 (device_metrics), len 0
        let ls = encode_json("Telemetry", &json!({"local_stats": {}})).unwrap();
        assert_eq!(ls, vec![0x32, 0x00]); // field 6 (local_stats), len 0
        let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &t);
        let d = encode_json("Data", &json!({"portnum": "TELEMETRY_APP", "payload": b64, "want_response": true})).unwrap();
        assert_eq!(d, vec![0x08, 0x43, 0x12, 0x02, 0x12, 0x00, 0x18, 0x01]);
    }

    #[test]
    fn empty_requests() {
        assert!(encode_json("Position", &json!({})).unwrap().is_empty());
        assert!(encode_json("RouteDiscovery", &json!({})).unwrap().is_empty());
        assert!(encode_json("NeighborInfo", &json!({})).unwrap().is_empty());
    }
}
