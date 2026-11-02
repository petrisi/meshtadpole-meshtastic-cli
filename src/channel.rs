//! Channel keys, channel hash, AES-CTR payload crypto.

use aes::{Aes128, Aes256};
use anyhow::{Result, bail};
use base64::Engine;
use ctr::cipher::{KeyIvInit, StreamCipher};
use serde::{Deserialize, Serialize};

/// Firmware default channel key (PSK index 1).
pub const DEFAULT_KEY: [u8; 16] = [
    0xd4, 0xf1, 0xbb, 0x3a, 0x20, 0x29, 0x07, 0x59, 0xf0, 0xbc, 0xff, 0xab, 0xcf, 0x4e, 0x69, 0x01,
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelConfig {
    pub name: String,
    /// Base64 PSK as used by Meshtastic clients (`AQ==` = default key).
    pub psk: String,
}

#[derive(Debug, Clone)]
pub struct Channel {
    pub name: String,
    /// Expanded AES key; empty = unencrypted.
    pub key: Vec<u8>,
    pub hash: u8,
}

/// Expand a PSK per firmware rules: 0 bytes = no crypto, 1 byte = default-key index,
/// 2..15 bytes zero-padded to 16, 16/32 bytes = AES-128/256, 17..31 zero-padded to 32.
pub fn expand_psk(psk: &[u8]) -> Vec<u8> {
    match psk.len() {
        0 => Vec::new(),
        1 => match psk[0] {
            0 => Vec::new(),
            i => {
                let mut k = DEFAULT_KEY.to_vec();
                k[15] = k[15].wrapping_add(i - 1);
                k
            }
        },
        n if n < 16 => {
            let mut k = psk.to_vec();
            k.resize(16, 0);
            k
        }
        16 | 32 => psk.to_vec(),
        n if n < 32 => {
            let mut k = psk.to_vec();
            k.resize(32, 0);
            k
        }
        _ => psk[..32].to_vec(),
    }
}

pub fn xor_hash(b: &[u8]) -> u8 {
    b.iter().fold(0, |a, x| a ^ x)
}

impl Channel {
    pub fn from_config(c: &ChannelConfig) -> Result<Channel> {
        let psk = base64::engine::general_purpose::STANDARD.decode(c.psk.trim())?;
        let key = expand_psk(&psk);
        let hash = xor_hash(c.name.as_bytes()) ^ xor_hash(&key);
        Ok(Channel { name: c.name.clone(), key, hash })
    }
}

/// 128-bit CTR nonce: packet id (u64 LE) | from (u32 LE) | 0u32.
pub fn nonce(from: u32, id: u32) -> [u8; 16] {
    let mut n = [0u8; 16];
    n[0..8].copy_from_slice(&(id as u64).to_le_bytes());
    n[8..12].copy_from_slice(&from.to_le_bytes());
    n
}

/// AES-CTR encrypt/decrypt in place (symmetric). 32-bit big-endian counter in the last 4 bytes.
pub fn ctr_apply(key: &[u8], from: u32, id: u32, buf: &mut [u8]) -> Result<()> {
    let iv = nonce(from, id);
    match key.len() {
        0 => {}
        16 => ctr::Ctr32BE::<Aes128>::new(key.into(), &iv.into()).apply_keystream(buf),
        32 => ctr::Ctr32BE::<Aes256>::new(key.into(), &iv.into()).apply_keystream(buf),
        n => bail!("invalid key length {n}"),
    }
    Ok(())
}

/// djb2 hash used for frequency-slot selection from channel name.
pub fn djb2(s: &str) -> u32 {
    s.bytes().fold(5381u32, |h, c| h.wrapping_shl(5).wrapping_add(h).wrapping_add(c as u32))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn efl_hash() {
        let c = Channel::from_config(&ChannelConfig { name: "EdgeFastLow".into(), psk: "AQ==".into() }).unwrap();
        assert_eq!(c.hash, 0x55);
        assert_eq!(c.key, DEFAULT_KEY.to_vec());
    }
    #[test]
    fn longfast_hash() {
        let c = Channel::from_config(&ChannelConfig { name: "LongFast".into(), psk: "AQ==".into() }).unwrap();
        assert_eq!(c.hash, 0x08);
    }
}
