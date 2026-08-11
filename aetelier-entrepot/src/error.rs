#[derive(Debug, thiserror::Error)]
pub enum EntrepotError {
    #[error("http transport: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("http status {status} for {key}: {body}")]
    Status {
        status: u16,
        key: String,
        body: String,
    },
    #[error("list response parse: {0}")]
    ListParse(String),
    #[error("io on {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("decode {key}: {reason}")]
    Decode { key: String, reason: String },
    #[error("retries exhausted after {attempts} attempts for {key}: {last}")]
    Exhausted {
        attempts: u32,
        key: String,
        last: String,
    },
    #[error("missing credentials: {0}")]
    Credentials(String),
}
