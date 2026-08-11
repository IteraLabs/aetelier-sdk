use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

pub const EMPTY_PAYLOAD_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

#[derive(Clone)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("access_key", &self.access_key)
            .field("secret_key", &"<redacted>")
            .finish()
    }
}

pub struct SignedHeaders {
    pub authorization: String,
    pub x_amz_date: String,
}

pub fn uri_encode(input: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(input.len());
    for b in input.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b'/' if !encode_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn hex_sha256(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac =
        <Hmac<Sha256> as Mac>::new_from_slice(key).expect("hmac accepts any key size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn canonical_query(query: &[(String, String)]) -> String {
    let mut pairs: Vec<(String, String)> = query
        .iter()
        .map(|(k, v)| (uri_encode(k, true), uri_encode(v, true)))
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn canonical_header_value(v: &str) -> String {
    v.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn build_canonical(
    method: &str,
    host: &str,
    path: &str,
    query: &[(String, String)],
    extra_headers: &[(String, String)],
    payload_hash: &str,
    amz_date: &str,
) -> (String, String) {
    let mut headers: Vec<(String, String)> = extra_headers
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), canonical_header_value(v)))
        .collect();
    headers.push(("host".to_string(), canonical_header_value(host)));
    headers.push(("x-amz-date".to_string(), amz_date.to_string()));
    headers.sort();

    let canonical_headers: String =
        headers.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
    let signed_headers = headers
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");

    let canonical_request = format!(
        "{method}\n{}\n{}\n{canonical_headers}\n{signed_headers}\n{payload_hash}",
        uri_encode(path, false),
        canonical_query(query),
    );
    (canonical_request, signed_headers)
}

#[allow(clippy::too_many_arguments)]
pub fn sign(
    creds: &Credentials,
    method: &str,
    host: &str,
    path: &str,
    query: &[(String, String)],
    extra_headers: &[(String, String)],
    payload_hash: &str,
    region: &str,
    service: &str,
    when: DateTime<Utc>,
) -> SignedHeaders {
    let amz_date = when.format("%Y%m%dT%H%M%SZ").to_string();
    let date = when.format("%Y%m%d").to_string();

    let (canonical_request, signed_headers) = build_canonical(
        method,
        host,
        path,
        query,
        extra_headers,
        payload_hash,
        &amz_date,
    );

    let scope = format!("{date}/{region}/{service}/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex_sha256(canonical_request.as_bytes())
    );

    let k_date = hmac_sha256(
        format!("AWS4{}", creds.secret_key).as_bytes(),
        date.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes()));

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        creds.access_key
    );

    SignedHeaders {
        authorization,
        x_amz_date: amz_date,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn aws_doc_example() -> (Credentials, DateTime<Utc>) {
        (
            Credentials {
                access_key: "AKIDEXAMPLE".to_string(),
                secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_string(),
            },
            Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap(),
        )
    }

    #[test]
    fn uri_encoding_matches_the_sigv4_alphabet() {
        assert_eq!(uri_encode("AZaz09-_.~", true), "AZaz09-_.~");
        assert_eq!(uri_encode("a b", true), "a%20b");
        assert_eq!(uri_encode("a/b", true), "a%2Fb");
        assert_eq!(uri_encode("a/b", false), "a/b");
        assert_eq!(uri_encode("a=b&c", true), "a%3Db%26c");
    }

    #[test]
    fn canonical_request_hash_matches_the_aws_documented_vector() {
        let (canonical, signed_headers) = build_canonical(
            "GET",
            "iam.amazonaws.com",
            "/",
            &[
                ("Action".to_string(), "ListUsers".to_string()),
                ("Version".to_string(), "2010-05-08".to_string()),
            ],
            &[(
                "content-type".to_string(),
                "application/x-www-form-urlencoded; charset=utf-8".to_string(),
            )],
            EMPTY_PAYLOAD_SHA256,
            "20150830T123600Z",
        );
        assert_eq!(signed_headers, "content-type;host;x-amz-date");
        assert_eq!(
            hex_sha256(canonical.as_bytes()),
            "f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59",
            "canonical request was:\n{canonical}"
        );
    }

    #[test]
    fn signature_matches_the_aws_documented_vector() {
        let (creds, when) = aws_doc_example();
        let signed = sign(
            &creds,
            "GET",
            "iam.amazonaws.com",
            "/",
            &[
                ("Action".to_string(), "ListUsers".to_string()),
                ("Version".to_string(), "2010-05-08".to_string()),
            ],
            &[(
                "content-type".to_string(),
                "application/x-www-form-urlencoded; charset=utf-8".to_string(),
            )],
            EMPTY_PAYLOAD_SHA256,
            "us-east-1",
            "iam",
            when,
        );
        assert_eq!(signed.x_amz_date, "20150830T123600Z");
        assert_eq!(
            signed.authorization,
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, \
             SignedHeaders=content-type;host;x-amz-date, \
             Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
        );
    }

    #[test]
    fn query_pairs_sort_by_encoded_form() {
        let q = [
            ("b".to_string(), "2".to_string()),
            ("a".to_string(), "1".to_string()),
            ("a".to_string(), "0".to_string()),
        ];
        assert_eq!(canonical_query(&q), "a=0&a=1&b=2");
    }
}
