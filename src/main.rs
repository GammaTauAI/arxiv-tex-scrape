use async_compression::futures::bufread::GzipDecoder;
use async_tar::Archive; // For handling tar archives
use futures::{AsyncReadExt, StreamExt}; // For gzip decompression
use std::{error::Error, io::Read};

#[tokio::main]
async fn main() {}

pub struct TexFile {
    name: String,
    content: String,
}

// the asynchronous function to download and extract TeX files
async fn download_paper(id: &str) -> Result<Vec<TexFile>, Box<dyn Error>> {
    let url = format!("https://arxiv.org/e-print/{}.pdf", id);
    let response = reqwest::get(&url).await?;
    let bytes = response.bytes().await?;

    // find out if the file is gzipped
    let is_gzipped = bytes.starts_with(&[0x1f, 0x8b]);
    assert!(is_gzipped, "File is not gzipped?!?!?!");

    // decompress gzip
    let mut decoder = GzipDecoder::new(&bytes[..]);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed).await?;
    println!("Decompressed {} bytes", decompressed.len());

    // extract tar
    let archive = Archive::new(decompressed.as_slice());
    let mut files = Vec::new();
    let mut entries = archive.entries()?;
    while let Some(file) = entries.next().await {
        let mut file = file?;
        let name = file.path()?.to_str().unwrap().to_string();
        if !name.ends_with(".tex") {
            // only tex allowed here!
            continue;
        }
        let mut content = String::new();
        file.read_to_string(&mut content).await?;
        files.push(TexFile { name, content });
    }
    Ok(files)
}
