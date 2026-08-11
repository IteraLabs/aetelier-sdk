use async_trait::async_trait;
use quick_xml::Reader;
use quick_xml::events::Event;

use crate::error::EntrepotError;
use crate::pacing::Jitter;
use crate::retry::{
    RetryPolicy, Verdict, classify_status, is_retryable_transport, parse_retry_after,
};
use crate::sign::{Credentials, EMPTY_PAYLOAD_SHA256, sign};
use crate::source::{ObjectMeta, ObjectSource};

#[derive(Debug, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub requester_pays: bool,
    pub credentials: Option<Credentials>,
    pub endpoint: Option<String>,
}

impl S3Config {
    pub fn from_env(bucket: &str, region: &str) -> Result<Self, EntrepotError> {
        let access_key = std::env::var("AWS_ACCESS_KEY_ID").map_err(|_| {
            EntrepotError::Credentials("AWS_ACCESS_KEY_ID unset".to_string())
        })?;
        let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").map_err(|_| {
            EntrepotError::Credentials("AWS_SECRET_ACCESS_KEY unset".to_string())
        })?;
        Ok(Self {
            bucket: bucket.to_string(),
            region: region.to_string(),
            requester_pays: true,
            credentials: Some(Credentials {
                access_key,
                secret_key,
            }),
            endpoint: None,
        })
    }

    pub fn anonymous(bucket: &str, region: &str, endpoint: Option<String>) -> Self {
        Self {
            bucket: bucket.to_string(),
            region: region.to_string(),
            requester_pays: false,
            credentials: None,
            endpoint,
        }
    }

    fn host(&self) -> String {
        match &self.endpoint {
            Some(e) => e
                .trim_start_matches("https://")
                .trim_start_matches("http://")
                .trim_end_matches('/')
                .to_string(),
            None => format!("{}.s3.{}.amazonaws.com", self.bucket, self.region),
        }
    }

    fn base_url(&self) -> String {
        match &self.endpoint {
            Some(e) => e.trim_end_matches('/').to_string(),
            None => format!("https://{}", self.host()),
        }
    }
}

pub struct S3Client {
    cfg: S3Config,
    http: reqwest::Client,
    policy: RetryPolicy,
    pace: Jitter,
}

impl S3Client {
    pub fn new(cfg: S3Config) -> Self {
        Self {
            cfg,
            http: reqwest::Client::new(),
            policy: RetryPolicy::default(),
            pace: Jitter::default(),
        }
    }

    pub fn with_policy(mut self, policy: RetryPolicy, pace: Jitter) -> Self {
        self.policy = policy;
        self.pace = pace;
        self
    }

    fn signed_headers(
        &self,
        path: &str,
        query: &[(String, String)],
    ) -> Vec<(String, String)> {
        let Some(creds) = &self.cfg.credentials else {
            return Vec::new();
        };
        let mut extra = vec![(
            "x-amz-content-sha256".to_string(),
            EMPTY_PAYLOAD_SHA256.to_string(),
        )];
        if self.cfg.requester_pays {
            extra.push(("x-amz-request-payer".to_string(), "requester".to_string()));
        }
        let signed = sign(
            creds,
            "GET",
            &self.cfg.host(),
            path,
            query,
            &extra,
            EMPTY_PAYLOAD_SHA256,
            &self.cfg.region,
            "s3",
            chrono::Utc::now(),
        );
        extra.push(("authorization".to_string(), signed.authorization));
        extra.push(("x-amz-date".to_string(), signed.x_amz_date));
        extra
    }

    async fn get_with_retry(
        &self,
        label: &str,
        path: &str,
        query: &[(String, String)],
    ) -> Result<Vec<u8>, EntrepotError> {
        let mut attempt: u32 = 0;
        loop {
            self.pace.wait().await;
            let url = if query.is_empty() {
                format!("{}{}", self.cfg.base_url(), path)
            } else {
                let qs = query
                    .iter()
                    .map(|(k, v)| format!("{k}={}", crate::sign::uri_encode(v, true)))
                    .collect::<Vec<_>>()
                    .join("&");
                format!("{}{}?{}", self.cfg.base_url(), path, qs)
            };
            let mut req = self.http.get(&url);
            for (k, v) in self.signed_headers(path, query) {
                req = req.header(k, v);
            }
            match req.send().await {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    match classify_status(status) {
                        Verdict::Success => {
                            return resp
                                .bytes()
                                .await
                                .map(|b| b.to_vec())
                                .map_err(Into::into);
                        }
                        Verdict::RetryAfterBackoff => {
                            let hint = resp
                                .headers()
                                .get("retry-after")
                                .and_then(|v| v.to_str().ok())
                                .and_then(parse_retry_after);
                            tracing::warn!(label, status, attempt, "entrepot.s3.retry");
                            if self.policy.exhausted(attempt) {
                                return Err(EntrepotError::Exhausted {
                                    attempts: attempt + 1,
                                    key: label.to_string(),
                                    last: format!("http {status}"),
                                });
                            }
                            tokio::time::sleep(self.policy.delay_for(attempt, hint))
                                .await;
                            attempt += 1;
                        }
                        Verdict::Fatal => {
                            let body = resp.text().await.unwrap_or_default();
                            let body = body.chars().take(2048).collect::<String>();
                            return Err(EntrepotError::Status {
                                status,
                                key: label.to_string(),
                                body,
                            });
                        }
                    }
                }
                Err(err) => {
                    if !is_retryable_transport(&err) || self.policy.exhausted(attempt) {
                        return Err(err.into());
                    }
                    tracing::warn!(label, error = %err, attempt, "entrepot.s3.transport_retry");
                    tokio::time::sleep(self.policy.delay_for(attempt, None)).await;
                    attempt += 1;
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPage {
    pub objects: Vec<ObjectMeta>,
    pub is_truncated: bool,
    pub next_token: Option<String>,
}

pub fn parse_list_page(xml: &str) -> Result<ListPage, EntrepotError> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut objects = Vec::new();
    let mut is_truncated = false;
    let mut next_token: Option<String> = None;
    let mut path: Vec<String> = Vec::new();
    let mut key = String::new();
    let mut size: Option<u64> = None;
    let mut etag: Option<String> = None;
    let mut last_modified: Option<String> = None;

    loop {
        match reader
            .read_event()
            .map_err(|e| EntrepotError::ListParse(e.to_string()))?
        {
            Event::Start(tag) => {
                let name = String::from_utf8_lossy(tag.local_name().as_ref()).to_string();
                if name == "Contents" {
                    key.clear();
                    size = None;
                    etag = None;
                    last_modified = None;
                }
                path.push(name);
            }
            Event::End(tag) => {
                let name = String::from_utf8_lossy(tag.local_name().as_ref()).to_string();
                if name == "Contents"
                    && let Some(size) = size
                    && !key.is_empty()
                {
                    objects.push(ObjectMeta {
                        key: key.clone(),
                        size,
                        etag: etag.clone(),
                        last_modified: last_modified.clone(),
                    });
                }
                path.pop();
            }
            Event::Text(text) => {
                let value = text
                    .unescape()
                    .map_err(|e| EntrepotError::ListParse(e.to_string()))?
                    .into_owned();
                let in_contents = path.len() >= 2 && path[path.len() - 2] == "Contents";
                match path.last().map(String::as_str) {
                    Some("Key") if in_contents => key = value,
                    Some("Size") if in_contents => {
                        size = value.parse::<u64>().ok();
                    }
                    Some("ETag") if in_contents => {
                        etag = Some(value.trim_matches('"').to_string());
                    }
                    Some("LastModified") if in_contents => last_modified = Some(value),
                    Some("IsTruncated") => is_truncated = value == "true",
                    Some("NextContinuationToken") => next_token = Some(value),
                    _ => {}
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(ListPage {
        objects,
        is_truncated,
        next_token,
    })
}

#[async_trait]
impl ObjectSource for S3Client {
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, EntrepotError> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut query = vec![
                ("list-type".to_string(), "2".to_string()),
                ("prefix".to_string(), prefix.to_string()),
            ];
            if let Some(t) = &token {
                query.push(("continuation-token".to_string(), t.clone()));
            }
            let body = self.get_with_retry(prefix, "/", &query).await?;
            let text = String::from_utf8_lossy(&body);
            let page = parse_list_page(&text)?;
            out.extend(page.objects);
            if page.is_truncated
                && let Some(t) = page.next_token
            {
                token = Some(t);
            } else {
                break;
            }
        }
        Ok(out)
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, EntrepotError> {
        let path = format!("/{key}");
        self.get_with_retry(key, &path, &[]).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIST_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>hyperliquid-archive</Name>
  <Prefix>market_data/20230916/9/l2Book/</Prefix>
  <KeyCount>2</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>true</IsTruncated>
  <NextContinuationToken>1ueGcxLPRx1Tr</NextContinuationToken>
  <Contents>
    <Key>market_data/20230916/9/l2Book/BTC.lz4</Key>
    <LastModified>2023-10-01T12:00:00.000Z</LastModified>
    <ETag>&quot;9b2cf535f27731c974343645a3985328&quot;</ETag>
    <Size>1048576</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <Contents>
    <Key>market_data/20230916/9/l2Book/SOL.lz4</Key>
    <LastModified>2023-10-01T12:00:01.000Z</LastModified>
    <ETag>&quot;abcdef0123456789abcdef0123456789-3&quot;</ETag>
    <Size>524288</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
</ListBucketResult>"#;

    #[test]
    fn parses_a_v2_list_page() {
        let page = parse_list_page(LIST_XML).unwrap();
        assert!(page.is_truncated);
        assert_eq!(page.next_token.as_deref(), Some("1ueGcxLPRx1Tr"));
        assert_eq!(page.objects.len(), 2);
        assert_eq!(page.objects[0].key, "market_data/20230916/9/l2Book/BTC.lz4");
        assert_eq!(page.objects[0].size, 1_048_576);
        assert_eq!(
            page.objects[0].etag.as_deref(),
            Some("9b2cf535f27731c974343645a3985328")
        );
        assert_eq!(
            page.objects[1].etag.as_deref(),
            Some("abcdef0123456789abcdef0123456789-3")
        );
    }

    #[test]
    fn final_page_reports_no_token() {
        let xml = LIST_XML
            .replace(
                "<IsTruncated>true</IsTruncated>",
                "<IsTruncated>false</IsTruncated>",
            )
            .replace(
                "<NextContinuationToken>1ueGcxLPRx1Tr</NextContinuationToken>",
                "",
            );
        let page = parse_list_page(&xml).unwrap();
        assert!(!page.is_truncated);
        assert_eq!(page.next_token, None);
    }

    #[test]
    fn requester_pays_and_content_hash_ride_every_request() {
        let cfg = S3Config {
            bucket: "hyperliquid-archive".to_string(),
            region: "us-east-1".to_string(),
            requester_pays: true,
            credentials: Some(Credentials {
                access_key: "AKIDEXAMPLE".to_string(),
                secret_key: "secret".to_string(),
            }),
            endpoint: None,
        };
        let client = S3Client::new(cfg);
        let headers = client.signed_headers("/market_data/x.lz4", &[]);
        let names: Vec<&str> = headers.iter().map(|(k, _)| k.as_str()).collect();
        assert!(names.contains(&"x-amz-request-payer"));
        assert!(names.contains(&"x-amz-content-sha256"));
        assert!(names.contains(&"authorization"));
        assert!(names.contains(&"x-amz-date"));
        let auth = &headers
            .iter()
            .find(|(k, _)| k == "authorization")
            .unwrap()
            .1;
        assert!(auth.contains(
            "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-request-payer"
        ));
    }

    #[test]
    fn anonymous_mode_sends_no_headers_and_honors_a_path_style_endpoint() {
        let cfg = S3Config::anonymous(
            "public.bitmex.com",
            "eu-west-1",
            Some("https://s3-eu-west-1.amazonaws.com/public.bitmex.com".to_string()),
        );
        assert_eq!(
            cfg.base_url(),
            "https://s3-eu-west-1.amazonaws.com/public.bitmex.com"
        );
        assert!(!cfg.requester_pays);
        let client = S3Client::new(cfg);
        assert!(
            client
                .signed_headers("/data/trade/x.csv.gz", &[])
                .is_empty()
        );
    }

    #[test]
    fn virtual_hosted_url_derives_from_bucket_and_region() {
        let cfg = S3Config {
            bucket: "hyperliquid-archive".to_string(),
            region: "us-east-1".to_string(),
            requester_pays: true,
            credentials: Some(Credentials {
                access_key: "a".to_string(),
                secret_key: "s".to_string(),
            }),
            endpoint: None,
        };
        assert_eq!(cfg.host(), "hyperliquid-archive.s3.us-east-1.amazonaws.com");
        assert_eq!(
            cfg.base_url(),
            "https://hyperliquid-archive.s3.us-east-1.amazonaws.com"
        );
    }
}
