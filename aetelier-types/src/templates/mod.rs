//! Configuration templates and feature engineering templates.
//!
//! Loads and manages experimental configurations, exchange templates,
//! and feature definitions from TOML files.

use serde::Deserialize;

#[cfg(feature = "std")]
use std::{error::Error, fs};

use crate::templates::{
    exchanges::centralized::ExchangeConfig, experiments::ExpConfig,
    features::FeatureConfig,
};

/// Exchange configuration templates.
pub mod exchanges;
/// Experiment configuration templates.
pub mod experiments;
/// Feature definition templates.
pub mod features;

/// Master configuration loading experiments, exchanges, and features.
#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    /// List of experiment configurations.
    pub experiments: Vec<ExpConfig>,
    /// List of exchange configurations.
    pub exchanges: Vec<ExchangeConfig>,
    /// Optional list of feature configurations.
    pub features: Option<Vec<FeatureConfig>>,
}

impl Config {
    /// Load configuration from a TOML file (requires `std` feature).
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or parsed as TOML.
    #[cfg(feature = "std")]
    pub fn load_from_toml(file_route: &str) -> Result<Self, Box<dyn Error>> {
        let contents = fs::read_to_string(file_route)?;
        let config: Config = toml::from_str(&contents)?;
        Ok(config)
    }

    /// Parse configuration from an in-memory TOML string.
    ///
    /// Works in both native and WASM environments.
    pub fn from_toml_str(contents: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(contents)
    }
}
