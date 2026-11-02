//! Public-key crypto (firmware CryptoEngine::encryptCurve25519/decryptCurve25519).
//!
//! shared = SHA256(X25519(our_private, their_public))
//! AES-256-CCM, L=2 (13-byte nonce), 8-byte tag, no AAD.
//! nonce[0..8] = packet id (u64 LE), nonce[4..8] overwritten by extra_nonce (u32 LE),
//! nonce[8..12] = from (u32 LE), nonce[12] = 0.
//! Wire payload: ciphertext | tag(8) | extra_nonce(4 LE). Header channel hash = 0.

use aes::Aes256;
use anyhow::{Result, anyhow, bail};
use base64::Engine;
use ccm::aead::{AeadInPlace, KeyInit, generic_array::GenericArray};
use ccm::consts::{U8, U13};
use sha2::{Digest, Sha256};
use x25519_dalek::{PublicKey, StaticSecret};

pub const OVERHEAD: usize = 12;
type Cipher = ccm::Ccm<Aes256, U8, U13>;

#[derive(Clone)]
pub struct Identity {
    pub secret: StaticSecret,
    pub public: [u8; 32],
}

impl Identity {
    pub fn from_b64(private: &str) -> Result<Identity> {
        let b = base64::engine::general_purpose::STANDARD.decode(private.trim())?;
        let k: [u8; 32] = b.try_into().map_err(|_| anyhow!("private key must be 32 bytes"))?;
        let secret = StaticSecret::from(k);
        let public = PublicKey::from(&secret).to_bytes();
        Ok(Identity { secret, public })
    }

    pub fn public_b64(&self) -> String {
        base64::engine::general_purpose::STANDARD.encode(self.public)
    }

    pub fn shared(&self, their_public: &[u8; 32]) -> [u8; 32] {
        let dh = self.secret.diffie_hellman(&PublicKey::from(*their_public));
        Sha256::digest(dh.as_bytes()).into()
    }
}

fn nonce(from: u32, id: u32, extra: u32) -> [u8; 13] {
    let mut n = [0u8; 16];
    n[0..8].copy_from_slice(&(id as u64).to_le_bytes());
    n[8..12].copy_from_slice(&from.to_le_bytes());
    if extra != 0 {
        n[4..8].copy_from_slice(&extra.to_le_bytes());
    }
    n[..13].try_into().unwrap()
}

pub fn encrypt(shared: &[u8; 32], from: u32, id: u32, plain: &[u8], extra: u32) -> Result<Vec<u8>> {
    let c = Cipher::new(GenericArray::from_slice(shared));
    let mut buf = plain.to_vec();
    let n = nonce(from, id, extra);
    let tag = c
        .encrypt_in_place_detached(GenericArray::from_slice(&n), b"", &mut buf)
        .map_err(|_| anyhow!("ccm encrypt"))?;
    buf.extend_from_slice(&tag);
    buf.extend_from_slice(&extra.to_le_bytes());
    Ok(buf)
}

pub fn decrypt(shared: &[u8; 32], from: u32, id: u32, wire: &[u8]) -> Result<Vec<u8>> {
    if wire.len() <= OVERHEAD {
        bail!("too short for PKC");
    }
    let (body, rest) = wire.split_at(wire.len() - OVERHEAD);
    let tag = &rest[..8];
    let extra = u32::from_le_bytes(rest[8..12].try_into().unwrap());
    let c = Cipher::new(GenericArray::from_slice(shared));
    let mut buf = body.to_vec();
    c.decrypt_in_place_detached(GenericArray::from_slice(&nonce(from, id, extra)), b"", &mut buf, GenericArray::from_slice(tag))
        .map_err(|_| anyhow!("ccm auth failed"))?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roundtrip_and_symmetry() {
        let a = Identity::from_b64(&base64::engine::general_purpose::STANDARD.encode([3u8; 32])).unwrap();
        let b = Identity::from_b64(&base64::engine::general_purpose::STANDARD.encode([7u8; 32])).unwrap();
        let s1 = a.shared(&b.public);
        let s2 = b.shared(&a.public);
        assert_eq!(s1, s2);
        let ct = encrypt(&s1, 0x1234, 0xdeadbeef, b"hello", 0x55aa55aa).unwrap();
        assert_eq!(ct.len(), 5 + OVERHEAD);
        assert_eq!(decrypt(&s2, 0x1234, 0xdeadbeef, &ct).unwrap(), b"hello");
        assert!(decrypt(&s2, 0x1235, 0xdeadbeef, &ct).is_err());
    }
}
