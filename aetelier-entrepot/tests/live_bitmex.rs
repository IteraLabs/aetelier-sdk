use aetelier_entrepot::s3::{S3Client, S3Config};
use aetelier_entrepot::source::ObjectSource;

#[tokio::test]
#[ignore = "network: hits the public BitMEX S3 mirror; run with cargo test -- --ignored"]
async fn anonymous_list_and_get_against_the_public_bitmex_mirror() {
    let cfg = S3Config::anonymous(
        "public.bitmex.com",
        "eu-west-1",
        Some("https://s3-eu-west-1.amazonaws.com/public.bitmex.com".to_string()),
    );
    let client = S3Client::new(cfg);

    let objects = client.list("data/trade/20141122").await.unwrap();
    assert_eq!(objects.len(), 1, "one daily file expected: {objects:?}");
    let obj = &objects[0];
    assert_eq!(obj.key, "data/trade/20141122.csv.gz");
    assert!(obj.size > 0);
    assert!(obj.etag.is_some());

    let bytes = client.get(&obj.key).await.unwrap();
    assert_eq!(bytes.len() as u64, obj.size);
    assert_eq!(&bytes[..2], &[0x1f, 0x8b], "gzip magic expected");
}
