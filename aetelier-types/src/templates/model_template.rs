use crate::errors::BuildError;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub enum Models {
    Uniform,
    GBM,
    Hawkes,
    GD,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ModelConfig {
    pub id: Option<String>,
    pub label: Option<Models>,
    pub description: Option<String>,
    pub params_labels: Option<Vec<String>>,
    pub params_values: Option<Vec<f64>>,
    pub seed: Option<u64>,
}

impl ModelConfig {
    pub fn builder() -> ModelConfigBuilder {
        ModelConfigBuilder::new()
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct ModelConfigBuilder {
    pub id: Option<String>,
    pub label: Option<Models>,
    pub description: Option<String>,
    pub params_labels: Option<Vec<String>>,
    pub params_values: Option<Vec<f64>>,
    pub seed: Option<u64>,
}

impl ModelConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }

    pub fn label(mut self, label: Models) -> Self {
        self.label = Some(label);
        self
    }

    pub fn description(mut self, description: String) -> Self {
        self.description = Some(description);
        self
    }

    pub fn params_labels(mut self, params_labels: Vec<String>) -> Self {
        self.params_labels = Some(params_labels);
        self
    }

    pub fn params_values(mut self, params_values: Vec<f64>) -> Self {
        self.params_values = Some(params_values);
        self
    }

    pub fn build(self) -> Result<ModelConfig, BuildError> {
        let id = self.id.ok_or(BuildError::MissingField("Model's id"))?;
        let label = self.label.ok_or(BuildError::MissingField("Model's label"))?;
        let description = self.description.ok_or(BuildError::MissingField("Model's description"))?;
        let params_labels = self.params_labels.ok_or(BuildError::MissingField("Model's params_labels"))?;
        let params_values = self.params_values.ok_or(BuildError::MissingField("Model's params_values"))?;

        Ok(ModelConfig {
            id: Some(id),
            label: Some(label),
            description: Some(description),
            params_labels: Some(params_labels),
            params_values: Some(params_values),
            seed: self.seed,
        })
    }
}
