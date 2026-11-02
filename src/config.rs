//! Tool configuration (JSON file) and LoRa parameter derivation.

use crate::channel::{Channel, ChannelConfig, djb2};
use anyhow::{Context, Result, anyhow, bail};
use meshtadpole_radio::sx1262::LoraConfig;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub const SYNC_WORD: u8 = 0x2B;
pub const PREAMBLE_LEN: u16 = 16;
pub const DEFAULT_API: &str = "127.0.0.1:47403";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RadioConfig {
    /// Region code as in Meshtastic `Config.LoRaConfig.RegionCode` (EU_868, EU_433, US, ...).
    pub region: String,
    /// Bandwidth in kHz. Meshtastic shorthand accepted (62 = 62.5, 31 = 31.25).
    pub bandwidth_khz: f64,
    pub spreading_factor: u8,
    /// Coding rate denominator 5..8.
    pub coding_rate: u8,
    /// 1-based frequency slot; 0 = derive from primary channel name hash.
    pub frequency_slot: u32,
    #[serde(default)]
    pub frequency_offset_mhz: f64,
    /// Explicit center frequency; overrides region/slot.
    #[serde(default)]
    pub override_frequency_mhz: Option<f64>,
    pub tx_power_dbm: i8,
    pub rx_boosted_gain: bool,
}

/// Our node identity on the mesh.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Node number. Required for TX.
    pub num: Option<u32>,
    pub long_name: String,
    pub short_name: String,
    /// `Config.DeviceConfig.Role` name advertised in NodeInfo. This tool never rebroadcasts.
    pub role: String,
    /// `HardwareModel` name advertised in NodeInfo.
    pub hw_model: String,
    /// Base64 X25519 private key. Required for PKC (DMs, admin).
    pub private_key: Option<String>,
    /// Hop limit for packets we originate.
    pub hop_limit: u8,
    /// Max fraction of airtime we may transmit over a rolling hour (EU868 sub-band: 0.10).
    pub duty_cycle_limit: f64,
}

impl Default for NodeConfig {
    fn default() -> Self {
        NodeConfig {
            num: None,
            long_name: "meshtadpole".into(),
            short_name: "tadp".into(),
            role: "CLIENT_HIDDEN".into(),
            hw_model: "PORTDUINO".into(),
            private_key: None,
            hop_limit: 3,
            duty_cycle_limit: 0.10,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub node: NodeConfig,
    pub radio: RadioConfig,
    /// Channel 0 is primary. Received packets are matched against all by hash.
    pub channels: Vec<ChannelConfig>,
    /// Daemon API listen address.
    pub api: String,
    /// USB serial of the stick to use; None = first found.
    #[serde(default)]
    pub usb_serial: Option<String>,
}

impl Default for Config {
    /// EdgeFastLow (meshpirkanmaa.org).
    fn default() -> Self {
        Config {
            node: NodeConfig::default(),
            radio: RadioConfig {
                region: "EU_868".into(),
                bandwidth_khz: 62.5,
                spreading_factor: 8,
                coding_rate: 8,
                frequency_slot: 1,
                frequency_offset_mhz: 0.0,
                override_frequency_mhz: None,
                tx_power_dbm: 14,
                rx_boosted_gain: true,
            },
            channels: vec![ChannelConfig { name: "EdgeFastLow".into(), psk: "AQ==".into() }],
            api: DEFAULT_API.into(),
            usb_serial: None,
        }
    }
}

/// (start MHz, end MHz) per region. Subset of firmware RadioInterface.cpp regions with spacing=padding=0.
fn region_band(r: &str) -> Option<(f64, f64)> {
    Some(match r {
        "US" => (902.0, 928.0),
        "EU_433" => (433.0, 434.0),
        "EU_868" => (869.4, 869.65),
        "CN" => (470.0, 510.0),
        "JP" => (920.5, 923.5),
        "ANZ" => (915.0, 928.0),
        "KR" => (920.0, 923.0),
        "TW" => (920.0, 925.0),
        "RU" => (868.7, 869.2),
        "IN" => (865.0, 867.0),
        "NZ_865" => (864.0, 868.0),
        "TH" => (920.0, 925.0),
        "UA_433" => (433.0, 434.7),
        "UA_868" => (868.0, 868.6),
        "MY_433" => (433.0, 435.0),
        "MY_919" => (919.0, 924.0),
        "SG_923" => (917.0, 925.0),
        "PH_433" => (433.0, 434.7),
        "PH_868" => (868.0, 869.4),
        "PH_915" => (915.0, 918.0),
        "KZ_433" => (433.075, 434.775),
        "KZ_863" => (863.0, 868.0),
        "NP_865" => (865.0, 868.0),
        "BR_902" => (902.0, 907.5),
        _ => return None,
    })
}

pub fn normalize_bw_khz(bw: f64) -> f64 {
    match bw as u32 {
        31 => 31.25,
        62 => 62.5,
        200 => 203.125,
        400 => 406.25,
        800 => 812.5,
        1600 => 1625.0,
        _ => bw,
    }
}

impl Config {
    pub fn channels(&self) -> Result<Vec<Channel>> {
        self.channels.iter().map(Channel::from_config).collect()
    }

    /// Center frequency in MHz (firmware RadioInterface::applyModemConfig).
    pub fn frequency_mhz(&self) -> Result<f64> {
        let r = &self.radio;
        if let Some(f) = r.override_frequency_mhz {
            return Ok(f + r.frequency_offset_mhz);
        }
        let (start, end) = region_band(&r.region).ok_or_else(|| anyhow!("unsupported region {}", r.region))?;
        let bw = normalize_bw_khz(r.bandwidth_khz);
        let slot_width = bw / 1000.0;
        let slots = ((end - start) / slot_width).round() as u32;
        if slots == 0 {
            bail!("bandwidth {bw} kHz does not fit region {}", r.region);
        }
        let ch = if r.frequency_slot == 0 {
            let name = self.channels.first().map(|c| c.name.as_str()).unwrap_or("");
            djb2(name) % slots
        } else {
            (r.frequency_slot - 1).min(slots - 1)
        };
        Ok(start + bw / 2000.0 + ch as f64 * slot_width + r.frequency_offset_mhz)
    }

    pub fn lora(&self) -> Result<LoraConfig> {
        let bw = normalize_bw_khz(self.radio.bandwidth_khz);
        Ok(LoraConfig {
            freq_hz: (self.frequency_mhz()? * 1e6).round() as u32,
            bw_hz: (bw * 1000.0).round() as u32,
            sf: self.radio.spreading_factor,
            cr: self.radio.coding_rate,
            sync_word: SYNC_WORD,
            preamble_len: PREAMBLE_LEN,
            tx_power_dbm: self.radio.tx_power_dbm,
            tcxo_mv: 1800,
            rx_boosted_gain: self.radio.rx_boosted_gain,
            current_limit_ma: 140,
        })
    }

    pub fn load(path: &Path) -> Result<Config> {
        if !path.exists() {
            return Ok(Config::default());
        }
        let s = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        serde_json::from_str(&s).with_context(|| format!("parse {}", path.display()))
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(d) = path.parent() {
            std::fs::create_dir_all(d)?;
        }
        std::fs::write(path, serde_json::to_string_pretty(self)? + "\n")?;
        Ok(())
    }
}

pub fn default_dir() -> PathBuf {
    dirs::data_local_dir().unwrap_or_else(|| PathBuf::from(".")).join("meshtadpole")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn efl_frequency() {
        let c = Config::default();
        assert!((c.frequency_mhz().unwrap() - 869.43125).abs() < 1e-6);
        assert_eq!(c.lora().unwrap().freq_hz, 869_431_250);
    }
}
