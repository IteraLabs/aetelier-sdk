use aetelier_entrepot::source::ObjectSource;
use aetelier_entrepot::{S3Client, S3Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let bucket = args
        .next()
        .expect("usage: probe <bucket> <region> <out_dir> <key>...");
    let region = args.next().expect("region");
    let out_dir = std::path::PathBuf::from(args.next().expect("out_dir"));
    let keys: Vec<String> = args.collect();
    assert!(!keys.is_empty(), "at least one key");

    let client = S3Client::new(S3Config::from_env(&bucket, &region)?);
    std::fs::create_dir_all(&out_dir)?;

    for key in &keys {
        match client.get(key).await {
            Ok(bytes) => {
                let dest = out_dir.join(key.replace('/', "_"));
                std::fs::write(&dest, &bytes)?;
                println!("OK   {key}  {} bytes  -> {}", bytes.len(), dest.display());
            }
            Err(e) => {
                println!("ERR  {key}  {e}");
            }
        }
    }
    Ok(())
}
