use serde::Deserialize;

/// Configuration for an experiment.
#[derive(Debug, Deserialize, Clone)]
pub struct ExpConfig {
    /// Unique experiment identifier.
    pub id: String,
    /// Number of progressions to run.
    pub n_progressions: u32,
    /// Optional number of agents participating.
    pub n_agents: Option<u32>,
}
