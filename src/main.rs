use async_compression::futures::bufread::GzipDecoder;
use async_tar::Archive; // For handling tar archives
use futures::{AsyncReadExt, StreamExt}; // For gzip decompression
use std::sync::Arc;
use std::{error::Error, io::Read};
use tokio::sync::mpsc;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 2 {
        panic!("Usage: cargo run -- <num_workers>");
    }
    let num_workers = args[1].parse::<usize>().unwrap();

    let mut workers = Vec::new();
    let (tb_tx, tb_rx) = mpsc::channel(num_workers);
    let (ds_tx, ds_rx) = mpsc::channel(num_workers);
    let tb_rx = Arc::new(tokio::sync::Mutex::new(tb_rx));
    for w_id in 0..num_workers {
        let spawned = spawn_transfer_worker(tb_rx.clone(), ds_tx.clone(), w_id);
        workers.push(spawned);
    }

    let ds_worker = spawn_ds_worker(ds_rx);
    // read $CARGO_MANIFEST_DIR/arxiv_ids.json (json array of strings) and
    // send each paper id to the transfer workers
    let mut ids = Vec::new();
    {
        let file = std::fs::File::open("arxiv_ids.json").unwrap();
        let reader = std::io::BufReader::new(file);
        let json: serde_json::Value = serde_json::from_reader(reader).unwrap();
        for id in json.as_array().unwrap() {
            ids.push(id.as_str().unwrap().to_string());
        }
    }

    println!("Read {} ids", ids.len());

    for id in ids {
        tb_tx.send(id).await.unwrap();
    }

    println!("All ids sent -- waiting.");

    // close channels to signal workers to exit and wait for them to exit
    drop(tb_tx);
    drop(ds_tx);
    for worker in workers {
        worker.await.unwrap();
    }
    ds_worker.await.unwrap();
}

#[derive(Debug, Clone)]
pub struct TexFile {
    name: String,
    content: String,
}

#[derive(Debug, Clone)]
pub struct Paper {
    id: String,
    files: Vec<TexFile>,
}

// the asynchronous function to download and extract TeX files
async fn download_paper(id: &str) -> Result<Paper, Box<dyn Error>> {
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
    Ok(Paper {
        id: id.to_string(),
        files,
    })
}

pub fn spawn_transfer_worker(
    rx: Arc<Mutex<mpsc::Receiver<String>>>, // if we close this channel, the workers will exit
    ds_tx: mpsc::Sender<Paper>,
    worker_id: usize,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn(async move {
        println!("Spawned transfer worker {}", worker_id);
        loop {
            let paper_id = {
                let mut rx = rx.lock().await;
                match rx.recv().await {
                    Some(t) => t,
                    None => {
                        println!("Transfer worker {} exiting", worker_id);
                        return;
                    }
                }
            };
            println!("[{}] Got paper {}", worker_id, paper_id);

            let paper = match download_paper(&paper_id).await {
                Ok(p) => p,
                Err(e) => {
                    println!(
                        "[{}] Error downloading paper {}: {}",
                        worker_id, paper_id, e
                    );
                    continue;
                }
            };

            if (ds_tx.send(paper).await).is_err() {
                return;
            }
        }
    })
}

pub fn spawn_ds_worker(mut rx: mpsc::Receiver<Paper>) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn(async move {
        println!("Spawned DS worker");
        loop {
            let paper = match rx.recv().await {
                Some(t) => t,
                None => {
                    println!("DS worker exiting");
                    return;
                }
            };
            println!("Got {} paper to store", paper.id);
            // do something with the papers
        }
    })
}
