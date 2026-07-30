use crate::errors::BuildError;
use serde::Deserialize;

/// Available feature types for configuration templates.
#[derive(Debug, Deserialize, Clone)]
pub enum Features {
    /// Orderbook feature type.
    OB,
}

/// Configuration for a single feature within a template.
#[derive(Debug, Deserialize, Clone)]
pub struct FeatureConfig {
    /// Unique identifier for this feature configuration.
    pub id: Option<String>,
    /// The feature type label.
    pub label: Option<Features>,
    /// Human-readable description of the feature.
    pub description: Option<String>,
    /// Labels for the feature parameters.
    pub params_labels: Option<Vec<String>>,
    /// Numeric values for the feature parameters.
    pub params_values: Option<Vec<f64>>,
}

impl FeatureConfig {
    /// Returns a new [`FeatureConfigBuilder`] for constructing a `FeatureConfig`.
    pub fn builder() -> FeatureConfigBuilder {
        FeatureConfigBuilder::new()
    }
}

/// Builder for constructing [`FeatureConfig`] instances with validation.
#[derive(Debug, Deserialize, Clone)]
pub struct FeatureConfigBuilder {
    /// Unique identifier for the feature.
    pub id: Option<String>,
    /// The feature type label.
    pub label: Option<Features>,
    /// Human-readable description.
    pub description: Option<String>,
    /// Labels for the feature parameters.
    pub params_labels: Option<Vec<String>>,
    /// Numeric values for the feature parameters.
    pub params_values: Option<Vec<f64>>,
}

impl Default for FeatureConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl FeatureConfigBuilder {
    /// Creates a new builder with all fields set to `None`.
    pub fn new() -> Self {
        FeatureConfigBuilder {
            id: None,
            label: None,
            description: None,
            params_labels: None,
            params_values: None,
        }
    }

    /// Sets the feature identifier.
    pub fn id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }

    /// Sets the feature type label.
    pub fn label(mut self, label: Features) -> Self {
        self.label = Some(label);
        self
    }

    /// Sets the feature description.
    pub fn description(mut self, description: String) -> Self {
        self.description = Some(description);
        self
    }

    /// Sets the parameter labels.
    pub fn params_labels(mut self, params_labels: Vec<String>) -> Self {
        self.params_labels = Some(params_labels);
        self
    }

    /// Sets the parameter values.
    pub fn params_values(mut self, params_values: Vec<f64>) -> Self {
        self.params_values = Some(params_values);
        self
    }

    /// Builds the [`FeatureConfig`], returning an error if any required field is missing.
    pub fn build(self) -> Result<FeatureConfig, BuildError> {
        let id = self.id.ok_or(BuildError::MissingField("Feature's id"))?;
        let label = self
            .label
            .ok_or(BuildError::MissingField("Features's label"))?;
        let description = self
            .description
            .ok_or(BuildError::MissingField("Feature's description"))?;
        let params_labels = self
            .params_labels
            .ok_or(BuildError::MissingField("Features's params_labels"))?;
        let params_values = self
            .params_values
            .ok_or(BuildError::MissingField("Features's params_values"))?;

        Ok(FeatureConfig {
            id: Some(id),
            label: Some(label),
            description: Some(description),
            params_labels: Some(params_labels),
            params_values: Some(params_values),
        })
    }
}
