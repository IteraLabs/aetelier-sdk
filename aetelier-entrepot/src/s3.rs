use async_trait::async_trait;
use quick_xml::Reader;
use quick_xml::events::Event;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::error::EntrepotError;
use crate::pacing::Jitter;
use crate::retry::{
    RetryPolicy, Verdict, classify_status, is_retryable_transport, parse_retry_after,
};
use crate::sign::{Credentials, EMPTY_PAYLOAD_SHA256, sign};
use crate::source::{ObjectMeta, ObjectSource};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedObject {
    pub bytes: Vec<u8>,
    pub etag: Option<String>,
    pub request_charged: bool,
}

impl FetchedObject {
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            etag: None,
            request_charged: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransferSnapshot {
    pub get_requests: u64,
    pub list_requests: u64,
    pub retries: u64,
    pub bytes_in: u64,
    pub unpaid_responses: u64,
    pub integrity_fail: u64,
}

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

#[derive(Debug, Default)]
pub struct TransferStats {
    get_requests: AtomicU64,
    list_requests: AtomicU64,
    retries: AtomicU64,
    bytes_in: AtomicU64,
    unpaid_responses: AtomicU64,
    integrity_fail: AtomicU64,
}

impl TransferStats {
    pub fn snapshot(&self) -> TransferSnapshot {
        TransferSnapshot {
            get_requests: self.get_requests.load(Ordering::Relaxed),
            list_requests: self.list_requests.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            unpaid_responses: self.unpaid_responses.load(Ordering::Relaxed),
            integrity_fail: self.integrity_fail.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum RequestKind {
    Get,
    List,
}

#[derive(Debug, Clone, Default)]
struct RespMeta {
    etag: Option<String>,
    content_length: Option<u64>,
    request_charged: bool,
}

pub fn verify_integrity(
    key: &str,
    bytes: &[u8],
    etag: Option<&str>,
    content_length: Option<u64>,
) -> Result<(), EntrepotError> {
    if let Some(cl) = content_length
        && cl != bytes.len() as u64
    {
        return Err(EntrepotError::Integrity {
            key: key.to_string(),
            reason: format!("body {} bytes vs content-length {cl}", bytes.len()),
        });
    }
    if let Some(tag) = etag
        && tag.len() == 32
        && tag.bytes().all(|b| b.is_ascii_hexdigit())
    {
        use md5::Digest;
        let digest = hex::encode(md5::Md5::digest(bytes));
        if !digest.eq_ignore_ascii_case(tag) {
            return Err(EntrepotError::Integrity {
                key: key.to_string(),
                reason: format!("md5 {digest} vs etag {tag}"),
            });
        }
    }
    Ok(())
}

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const IDLE_READ_TIMEOUT: Duration = Duration::from_secs(30);

fn http_client(connect: Duration, idle_read: Duration) -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(connect)
        .read_timeout(idle_read)
        .build()
        .expect("default tls backend builds")
}

pub struct S3Client {
    cfg: S3Config,
    http: reqwest::Client,
    policy: RetryPolicy,
    pace: Jitter,
    stats: Arc<TransferStats>,
}

impl S3Client {
    pub fn new(cfg: S3Config) -> Self {
        Self {
            cfg,
            http: http_client(CONNECT_TIMEOUT, IDLE_READ_TIMEOUT),
            policy: RetryPolicy::default(),
            pace: Jitter::default(),
            stats: Arc::new(TransferStats::default()),
        }
    }

    pub fn with_policy(mut self, policy: RetryPolicy, pace: Jitter) -> Self {
        self.policy = policy;
        self.pace = pace;
        self
    }

    #[cfg(test)]
    fn with_deadlines(mut self, connect: Duration, idle_read: Duration) -> Self {
        self.http = http_client(connect, idle_read);
        self
    }

    pub fn stats(&self) -> TransferSnapshot {
        self.stats.snapshot()
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

    async fn retry_transport(
        &self,
        err: reqwest::Error,
        label: &str,
        attempt: u32,
    ) -> Option<EntrepotError> {
        if !is_retryable_transport(&err) || self.policy.exhausted(attempt) {
            return Some(err.into());
        }
        tracing::warn!(label, error = %err, attempt, "entrepot.s3.transport_retry");
        self.stats.retries.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(self.policy.delay_for(attempt, None)).await;
        None
    }

    async fn get_with_retry(
        &self,
        kind: RequestKind,
        label: &str,
        path: &str,
        query: &[(String, String)],
    ) -> Result<(Vec<u8>, RespMeta), EntrepotError> {
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
            match kind {
                RequestKind::Get => {
                    self.stats.get_requests.fetch_add(1, Ordering::Relaxed)
                }
                RequestKind::List => {
                    self.stats.list_requests.fetch_add(1, Ordering::Relaxed)
                }
            };
            match req.send().await {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    match classify_status(status) {
                        Verdict::Success => {
                            let header = |name: &str| {
                                resp.headers()
                                    .get(name)
                                    .and_then(|v| v.to_str().ok())
                                    .map(|v| v.to_string())
                            };
                            let meta = RespMeta {
                                etag: header("etag")
                                    .map(|t| t.trim_matches('"').to_string()),
                                content_length: header("content-length")
                                    .and_then(|v| v.parse().ok()),
                                request_charged: header("x-amz-request-charged")
                                    .is_some_and(|v| v == "requester"),
                            };
                            if self.cfg.requester_pays
                                && self.cfg.credentials.is_some()
                                && !meta.request_charged
                            {
                                self.stats
                                    .unpaid_responses
                                    .fetch_add(1, Ordering::Relaxed);
                                tracing::warn!(
                                    label,
                                    "entrepot.s3.request_charged_missing"
                                );
                            }
                            match resp.bytes().await {
                                Ok(body) => {
                                    let body = body.to_vec();
                                    self.stats
                                        .bytes_in
                                        .fetch_add(body.len() as u64, Ordering::Relaxed);
                                    return Ok((body, meta));
                                }
                                Err(err) => {
                                    if let Some(fatal) =
                                        self.retry_transport(err, label, attempt).await
                                    {
                                        return Err(fatal);
                                    }
                                    attempt += 1;
                                }
                            }
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
                            self.stats.retries.fetch_add(1, Ordering::Relaxed);
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
                    if let Some(fatal) = self.retry_transport(err, label, attempt).await {
                        return Err(fatal);
                    }
                    attempt += 1;
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPage {
    pub objects: Vec<ObjectMeta>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_token: Option<String>,
}

pub fn parse_list_page(xml: &str) -> Result<ListPage, EntrepotError> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut objects = Vec::new();
    let mut common_prefixes = Vec::new();
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
                let decoded = text
                    .decode()
                    .map_err(|e| EntrepotError::ListParse(e.to_string()))?;
                let value = quick_xml::escape::unescape(&decoded)
                    .map_err(|e| EntrepotError::ListParse(e.to_string()))?
                    .into_owned();
                let in_contents = path.len() >= 2 && path[path.len() - 2] == "Contents";
                let in_common =
                    path.len() >= 2 && path[path.len() - 2] == "CommonPrefixes";
                match path.last().map(String::as_str) {
                    Some("Prefix") if in_common => common_prefixes.push(value),
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
        common_prefixes,
        is_truncated,
        next_token,
    })
}

impl S3Client {
    pub async fn list_delimited(
        &self,
        prefix: &str,
        delimiter: &str,
    ) -> Result<ListPage, EntrepotError> {
        let query = vec![
            ("delimiter".to_string(), delimiter.to_string()),
            ("list-type".to_string(), "2".to_string()),
            ("prefix".to_string(), prefix.to_string()),
        ];
        let (body, _) = self
            .get_with_retry(RequestKind::List, prefix, "/", &query)
            .await?;
        parse_list_page(&String::from_utf8_lossy(&body))
    }
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
            let (body, _) = self
                .get_with_retry(RequestKind::List, prefix, "/", &query)
                .await?;
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
        self.get_object(key).await.map(|f| f.bytes)
    }
}

impl S3Client {
    pub async fn get_object(&self, key: &str) -> Result<FetchedObject, EntrepotError> {
        let path = format!("/{key}");
        let (bytes, meta) = self
            .get_with_retry(RequestKind::Get, key, &path, &[])
            .await?;
        if let Err(e) =
            verify_integrity(key, &bytes, meta.etag.as_deref(), meta.content_length)
        {
            self.stats.integrity_fail.fetch_add(1, Ordering::Relaxed);
            tracing::error!(key, error = %e, "entrepot.s3.integrity_fail");
            return Err(e);
        }
        Ok(FetchedObject {
            bytes,
            etag: meta.etag,
            request_charged: meta.request_charged,
        })
    }

    pub fn transfer_snapshot(&self) -> Option<TransferSnapshot> {
        Some(self.stats())
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
    fn delimited_list_yields_common_prefixes() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>hyperliquid-archive</Name>
  <Prefix></Prefix>
  <Delimiter>/</Delimiter>
  <KeyCount>3</KeyCount>
  <IsTruncated>false</IsTruncated>
  <CommonPrefixes><Prefix>asset_ctxs/</Prefix></CommonPrefixes>
  <CommonPrefixes><Prefix>market_data/</Prefix></CommonPrefixes>
  <Contents>
    <Key>README.md</Key>
    <LastModified>2023-10-01T12:00:00.000Z</LastModified>
    <ETag>&quot;9b2cf535f27731c974343645a3985328&quot;</ETag>
    <Size>128</Size>
  </Contents>
</ListBucketResult>"#;
        let page = parse_list_page(xml).unwrap();
        assert_eq!(page.common_prefixes, ["asset_ctxs/", "market_data/"]);
        assert_eq!(page.objects.len(), 1);
        assert_eq!(page.objects[0].key, "README.md");
        assert!(!page.is_truncated);
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

    fn http_response(status: u16, headers: &[(&str, &str)], body: &[u8]) -> Vec<u8> {
        let mut head = format!(
            "HTTP/1.1 {status} MOCK\r\nconnection: close\r\ncontent-length: {}\r\n",
            body.len()
        );
        for (k, v) in headers {
            head.push_str(&format!("{k}: {v}\r\n"));
        }
        head.push_str("\r\n");
        let mut out = head.into_bytes();
        out.extend_from_slice(body);
        out
    }

    fn truncated_http_response(declared_len: usize, sent: &[u8]) -> Vec<u8> {
        let mut out = format!(
            "HTTP/1.1 200 MOCK\r\nconnection: close\r\ncontent-length: {declared_len}\r\n\r\n"
        )
        .into_bytes();
        out.extend_from_slice(sent);
        out
    }

    async fn read_request_head(sock: &mut tokio::net::TcpStream) {
        let mut buf = [0u8; 8192];
        let mut seen: Vec<u8> = Vec::new();
        loop {
            let n = tokio::io::AsyncReadExt::read(sock, &mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            seen.extend_from_slice(&buf[..n]);
            if seen.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
    }

    async fn serve(
        responses: Vec<Vec<u8>>,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            for resp in responses {
                let (mut sock, _) = listener.accept().await.unwrap();
                read_request_head(&mut sock).await;
                tokio::io::AsyncWriteExt::write_all(&mut sock, &resp)
                    .await
                    .unwrap();
                tokio::io::AsyncWriteExt::shutdown(&mut sock).await.ok();
            }
        });
        (addr, handle)
    }

    async fn serve_stalled_body_then(
        hold: Duration,
        second: Vec<u8>,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut stalled, _) = listener.accept().await.unwrap();
            read_request_head(&mut stalled).await;
            tokio::io::AsyncWriteExt::write_all(
                &mut stalled,
                &truncated_http_response(13, b""),
            )
            .await
            .unwrap();
            let held = tokio::spawn(async move {
                tokio::time::sleep(hold).await;
                drop(stalled);
            });
            let (mut sock, _) = listener.accept().await.unwrap();
            read_request_head(&mut sock).await;
            tokio::io::AsyncWriteExt::write_all(&mut sock, &second)
                .await
                .unwrap();
            tokio::io::AsyncWriteExt::shutdown(&mut sock).await.ok();
            held.await.unwrap();
        });
        (addr, handle)
    }

    fn fast_client(addr: std::net::SocketAddr, requester_pays: bool) -> S3Client {
        let cfg = S3Config {
            bucket: "mock".to_string(),
            region: "us-east-1".to_string(),
            requester_pays,
            credentials: Some(Credentials {
                access_key: "AKIDEXAMPLE".to_string(),
                secret_key: "secret".to_string(),
            }),
            endpoint: Some(format!("http://{addr}")),
        };
        S3Client::new(cfg).with_policy(
            RetryPolicy {
                max_retries: 2,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(5),
                multiplier: 2.0,
            },
            Jitter::new(Duration::from_millis(1), Duration::from_millis(2)),
        )
    }

    fn md5_hex(body: &[u8]) -> String {
        use md5::Digest;
        hex::encode(md5::Md5::digest(body))
    }

    #[tokio::test]
    async fn get_object_captures_etag_charge_and_counts_bytes() {
        let body = b"payload-bytes".to_vec();
        let etag = md5_hex(&body);
        let (addr, server) = serve(vec![http_response(
            200,
            &[
                ("etag", &format!("\"{etag}\"")),
                ("x-amz-request-charged", "requester"),
            ],
            &body,
        )])
        .await;
        let client = fast_client(addr, true);
        let fetched = client.get_object("market_data/x.lz4").await.unwrap();
        server.await.unwrap();
        assert_eq!(fetched.bytes, body);
        assert_eq!(fetched.etag.as_deref(), Some(etag.as_str()));
        assert!(fetched.request_charged);
        let stats = client.stats();
        assert_eq!(stats.get_requests, 1);
        assert_eq!(stats.bytes_in, body.len() as u64);
        assert_eq!(stats.unpaid_responses, 0);
        assert_eq!(stats.integrity_fail, 0);
    }

    #[tokio::test]
    async fn requester_pays_response_without_charge_header_counts_unpaid() {
        let body = b"paid?".to_vec();
        let etag = md5_hex(&body);
        let (addr, server) = serve(vec![http_response(
            200,
            &[("etag", &format!("\"{etag}\""))],
            &body,
        )])
        .await;
        let client = fast_client(addr, true);
        let fetched = client.get_object("k").await.unwrap();
        server.await.unwrap();
        assert!(!fetched.request_charged);
        assert_eq!(client.stats().unpaid_responses, 1);
    }

    #[tokio::test]
    async fn etag_mismatch_classifies_as_integrity_not_decode() {
        let (addr, server) = serve(vec![http_response(
            200,
            &[("etag", "\"00000000000000000000000000000000\"")],
            b"corrupted-in-flight",
        )])
        .await;
        let client = fast_client(addr, false);
        let err = client.get_object("k").await.unwrap_err();
        server.await.unwrap();
        assert!(matches!(err, EntrepotError::Integrity { .. }));
        assert_eq!(client.stats().integrity_fail, 1);
    }

    #[tokio::test]
    async fn transient_status_retries_then_succeeds_and_counts() {
        let body = b"eventually".to_vec();
        let etag = md5_hex(&body);
        let (addr, server) = serve(vec![
            http_response(503, &[], b"slow down"),
            http_response(200, &[("etag", &format!("\"{etag}\""))], &body),
        ])
        .await;
        let client = fast_client(addr, false);
        let fetched = client.get_object("k").await.unwrap();
        server.await.unwrap();
        assert_eq!(fetched.bytes, body);
        let stats = client.stats();
        assert_eq!(stats.get_requests, 2);
        assert_eq!(stats.retries, 1);
    }

    #[tokio::test]
    async fn body_read_timeout_retries_instead_of_terminating_the_run() {
        let body = b"after-a-stall".to_vec();
        let etag = md5_hex(&body);
        let hold = Duration::from_secs(1);
        let (addr, server) = serve_stalled_body_then(
            hold,
            http_response(200, &[("etag", &format!("\"{etag}\""))], &body),
        )
        .await;
        let client = fast_client(addr, false)
            .with_deadlines(CONNECT_TIMEOUT, Duration::from_millis(50));
        let started = std::time::Instant::now();
        let fetched = client.get_object("k").await.unwrap();
        let elapsed = started.elapsed();
        server.await.unwrap();
        assert_eq!(fetched.bytes, body);
        assert!(
            elapsed < hold / 2,
            "read deadline did not fire: {elapsed:?}"
        );
        let stats = client.stats();
        assert_eq!(stats.get_requests, 2);
        assert_eq!(stats.retries, 1);
        assert_eq!(stats.bytes_in, body.len() as u64);
    }

    #[tokio::test]
    async fn truncated_body_retries_instead_of_terminating_the_run() {
        let body = b"whole-object".to_vec();
        let etag = md5_hex(&body);
        let (addr, server) = serve(vec![
            truncated_http_response(64, b"half"),
            http_response(200, &[("etag", &format!("\"{etag}\""))], &body),
        ])
        .await;
        let client = fast_client(addr, false);
        let fetched = client.get_object("k").await.unwrap();
        server.await.unwrap();
        assert_eq!(fetched.bytes, body);
        assert_eq!(client.stats().retries, 1);
    }

    #[tokio::test]
    async fn repeated_body_failures_exhaust_the_transport_budget() {
        let (addr, server) = serve(vec![
            truncated_http_response(64, b"half"),
            truncated_http_response(64, b"half"),
            truncated_http_response(64, b"half"),
        ])
        .await;
        let client = fast_client(addr, false);
        let err = client.get_object("k").await.unwrap_err();
        server.await.unwrap();
        assert!(matches!(err, EntrepotError::Transport(_)));
        let stats = client.stats();
        assert_eq!(stats.get_requests, 3);
        assert_eq!(stats.retries, 2);
    }

    #[test]
    fn the_client_carries_an_idle_read_deadline_and_no_total_timeout() {
        let client = S3Client::new(S3Config::anonymous("mock", "us-east-1", None));
        let shape = format!("{:?}", client.http);
        assert!(shape.contains("read_timeout: 30s"), "{shape}");
        assert!(!shape.contains("TotalTimeout"), "{shape}");
    }

    #[tokio::test]
    async fn a_black_holed_connect_surfaces_instead_of_parking() {
        let cfg = S3Config::anonymous(
            "mock",
            "us-east-1",
            Some("http://192.0.2.1:81".to_string()),
        );
        let client = S3Client::new(cfg)
            .with_policy(
                RetryPolicy {
                    max_retries: 0,
                    base_delay: Duration::from_millis(1),
                    max_delay: Duration::from_millis(5),
                    multiplier: 2.0,
                },
                Jitter::new(Duration::from_millis(1), Duration::from_millis(2)),
            )
            .with_deadlines(Duration::from_millis(50), IDLE_READ_TIMEOUT);
        let started = std::time::Instant::now();
        let err = client.get_object("k").await.unwrap_err();
        let EntrepotError::Transport(source) = err else {
            panic!("expected a transport error");
        };
        assert!(source.is_connect() || source.is_timeout(), "{source}");
        assert!(started.elapsed() < Duration::from_secs(5), "parked");
    }

    #[test]
    fn integrity_checks_length_and_single_part_md5_only() {
        let body = b"twelve bytes";
        let good = md5_hex(body);
        assert!(verify_integrity("k", body, Some(&good), Some(12)).is_ok());
        assert!(matches!(
            verify_integrity("k", body, Some(&good), Some(13)),
            Err(EntrepotError::Integrity { .. })
        ));
        assert!(matches!(
            verify_integrity("k", body, Some("00000000000000000000000000000000"), None),
            Err(EntrepotError::Integrity { .. })
        ));
        assert!(
            verify_integrity("k", body, Some("multipart-etag-0000-3"), Some(12)).is_ok()
        );
        let foreign = md5_hex(b"other bytes entirely");
        let aws_multipart = format!("{foreign}-2");
        assert!(verify_integrity("k", body, Some(&aws_multipart), Some(12)).is_ok());
        assert!(verify_integrity("k", body, Some(&aws_multipart), Some(13)).is_err());
        assert!(verify_integrity("k", body, None, None).is_ok());
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
