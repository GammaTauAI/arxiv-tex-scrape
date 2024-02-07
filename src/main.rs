use async_compression::futures::bufread::GzipDecoder;
use async_tar::Archive; // For handling tar archives
use futures::{AsyncReadExt, StreamExt};
use rand::seq::SliceRandom; // For gzip decompression
use std::error::Error;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

lazy_static::lazy_static! {
    static ref ATOMIC_COUNTER: AtomicUsize = AtomicUsize::new(0);
    static ref ATOMIC_MAX: AtomicUsize = AtomicUsize::new(0);
}

#[tokio::main]
async fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 3 {
        panic!("Usage: cargo run -- <num_workers> <path to store>");
    }
    let num_workers = args[1].parse::<usize>().unwrap();
    let path = args[2].clone();

    let mut workers = Vec::new();
    let (tb_tx, tb_rx) = mpsc::channel(num_workers);
    let (ds_tx, ds_rx) = mpsc::channel(num_workers);
    let tb_rx = Arc::new(tokio::sync::Mutex::new(tb_rx));
    for w_id in 0..num_workers {
        let spawned = spawn_transfer_worker(tb_rx.clone(), ds_tx.clone(), w_id);
        workers.push(spawned);
    }

    let ds_worker = spawn_ds_worker(ds_rx, path);
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
    let total_ids = ids.len();
    ATOMIC_MAX.store(total_ids, std::sync::atomic::Ordering::Relaxed);
    // shuffle the ids
    let mut rng = rand::thread_rng();
    ids.shuffle(&mut rng);

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
    loop {
        let url = format!("https://arxiv.org/e-print/{}.pdf", id);
        let response = reqwest::get(&url).await?;
        // check status code, if 429, wait and retry
        if response.status().as_u16() == 429 {
            println!("429: Waiting 120 seconds");
            tokio::time::sleep(std::time::Duration::from_secs(120)).await;
            continue;
        }

        let bytes = response.bytes().await?;

        // find out if the file is gzipped
        let is_gzipped = bytes.starts_with(&[0x1f, 0x8b]);
        if !is_gzipped {
            return Err("Not a gzip file".into());
        }

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
            let path = file.path()?;
            let name = path.to_str();
            if name.is_none() {
                continue;
            }
            let name = name.unwrap().to_string();
            if !name.ends_with(".tex") || name.contains('/') {
                // only tex allowed here and only top-level files
                continue;
            }
            let mut content = String::new();
            file.read_to_string(&mut content).await?;
            files.push(TexFile { name, content });
        }
        if files.is_empty() {
            return Err("No tex files found".into());
        }
        return Ok(Paper {
            id: id.to_string(),
            files,
        });
    }
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

pub fn spawn_ds_worker(
    mut rx: mpsc::Receiver<Paper>,
    root_path: String,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn(async move {
        // create the root path if it doesn't exist
        if let Err(e) = tokio::fs::create_dir_all(&root_path).await {
            println!("Error creating root directory {}: {}", root_path, e);
            return;
        }
        println!("Spawned DS worker");
        loop {
            let num = ATOMIC_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let max = ATOMIC_MAX.load(std::sync::atomic::Ordering::Relaxed);
            let paper = match rx.recv().await {
                Some(t) => t,
                None => {
                    println!("DS worker exiting");
                    return;
                }
            };
            println!("({}/{}) Got {} paper to store", num, max, paper.id);
            // create a directory for the paper
            let paper_path = format!("{}/{}", root_path, paper.id);
            if let Err(e) = tokio::fs::create_dir(&paper_path).await {
                println!("Error creating paper directory {}: {}", paper_path, e);
                continue;
            }

            // store the tex files
            for file in paper.files {
                let file_path = format!("{}/{}", paper_path, file.name);
                if let Err(e) = tokio::fs::write(&file_path, file.content).await {
                    println!("Error writing file {}: {}", file_path, e);
                }
            }
        }
    })
}
