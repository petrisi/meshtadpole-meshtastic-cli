//! Meshtastic over-the-air packet header (16 bytes, little-endian).
//!
//! | off | len | field |
//! |---|---|---|
//! | 0 | 4 | to |
//! | 4 | 4 | from |
//! | 8 | 4 | id |
//! | 12 | 1 | flags: hop_limit[2:0], want_ack[3], via_mqtt[4], hop_start[7:5] |
//! | 13 | 1 | channel hash |
//! | 14 | 1 | next_hop (last byte of node num, 0 = none) |
//! | 15 | 1 | relay_node (last byte of node num) |

use serde::Serialize;

pub const HEADER_LEN: usize = 16;
pub const BROADCAST: u32 = 0xFFFF_FFFF;

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub struct Header {
    pub to: u32,
    pub from: u32,
    pub id: u32,
    pub hop_limit: u8,
    pub want_ack: bool,
    pub via_mqtt: bool,
    pub hop_start: u8,
    pub channel_hash: u8,
    pub next_hop: u8,
    pub relay_node: u8,
}

impl Header {
    pub fn parse(b: &[u8]) -> Option<Header> {
        if b.len() < HEADER_LEN {
            return None;
        }
        let u = |o: usize| u32::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]]);
        let f = b[12];
        Some(Header {
            to: u(0),
            from: u(4),
            id: u(8),
            hop_limit: f & 0x07,
            want_ack: f & 0x08 != 0,
            via_mqtt: f & 0x10 != 0,
            hop_start: (f & 0xE0) >> 5,
            channel_hash: b[13],
            next_hop: b[14],
            relay_node: b[15],
        })
    }

    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut o = [0u8; HEADER_LEN];
        o[0..4].copy_from_slice(&self.to.to_le_bytes());
        o[4..8].copy_from_slice(&self.from.to_le_bytes());
        o[8..12].copy_from_slice(&self.id.to_le_bytes());
        o[12] = (self.hop_limit & 0x07)
            | (self.want_ack as u8) << 3
            | (self.via_mqtt as u8) << 4
            | (self.hop_start & 0x07) << 5;
        o[13] = self.channel_hash;
        o[14] = self.next_hop;
        o[15] = self.relay_node;
        o
    }

    /// Hops already traversed, if the sender populated hop_start (firmware >= 2.3).
    pub fn hops_taken(&self) -> Option<u8> {
        (self.hop_start > 0 && self.hop_start >= self.hop_limit).then(|| self.hop_start - self.hop_limit)
    }
}

pub fn node_id(n: u32) -> String {
    if n == BROADCAST { "^all".into() } else { format!("!{n:08x}") }
}

/// Parse `!abcd1234`, `abcd1234`, `0xabcd1234`, decimal, or `^all`.
pub fn parse_node_id(s: &str) -> Option<u32> {
    let s = s.trim();
    if s == "^all" {
        return Some(BROADCAST);
    }
    if let Some(h) = s.strip_prefix('!').or_else(|| s.strip_prefix("0x")) {
        return u32::from_str_radix(h, 16).ok();
    }
    if s.len() == 8 && s.chars().all(|c| c.is_ascii_hexdigit()) {
        return u32::from_str_radix(s, 16).ok();
    }
    s.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn header_roundtrip() {
        let raw = hex::decode("ffffffffd998af9a99629a8c605500c4").unwrap();
        let h = Header::parse(&raw).unwrap();
        assert_eq!(h.to, BROADCAST);
        assert_eq!(h.from, 0x9aaf98d9);
        assert_eq!(h.id, 0x8c9a6299);
        assert_eq!((h.hop_limit, h.hop_start, h.channel_hash, h.relay_node), (0, 3, 0x55, 0xc4));
        assert_eq!(h.encode().to_vec(), raw);
        assert_eq!(h.hops_taken(), Some(3));
    }
}
