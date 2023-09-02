use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use croissantine::database::Database;
use croissantine::text::cleanup_chars;
use croissantine::text::trigrams::TriGrams;
use flate2::read::MultiGzDecoder;
use heed::EnvOpenOptions;
use httparse::{Response, Status, EMPTY_HEADER};
use url::Url;
use warc::{RecordType, WarcHeader};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Options {
    /// The CommonCrawl Gzipped WARC file to analyze
    #[arg(value_name = "FILE")]
    file_path: PathBuf,

    /// The database path where the indexed data is stored.
    #[arg(long, default_value = "croissantine.db")]
    database_path: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let Options { file_path, database_path } = Options::parse();

    let mut options = EnvOpenOptions::new();
    options.map_size(100 * 1024 * 1024 * 1024); // 100GiB
    let database = Database::open_or_create(options, database_path);

    let file =
        File::open(&file_path).with_context(|| format!("while opening {}", file_path.display()))?;
    let reader = BufReader::new(file);
    let uncompressed = BufReader::new(MultiGzDecoder::new(reader));
    let warc = warc::WarcReader::new(uncompressed);

    for result in warc.iter_records() {
        let record = result?;
        if record.warc_type() == &RecordType::Response {
            if let Some(uri) = record.header(WarcHeader::TargetURI) {
                let url = Url::parse(&uri)?;
                println!("{}", url);

                let mut headers = [EMPTY_HEADER; 64];
                let mut req = Response::new(&mut headers);
                let http_body = record.body();
                if let Ok(Status::Complete(size)) = req.parse(http_body) {
                    let html_body = &http_body[size..];
                    let product = readability::extractor::extract(&mut &html_body[..], &url)?;
                    println!("{:?}", product.title);
                    println!();

                    if product.title.contains("Folie") {
                        println!("{}", product.text);

                        let mut map = std::collections::HashMap::<_, usize>::new();
                        let chars = cleanup_chars(product.text.chars());
                        let ngrams = TriGrams::new(chars);
                        ngrams.for_each(|trigram| *map.entry(trigram).or_default() += 1);

                        for (trigram, count) in map.into_iter() {
                            println!("  {:?} => {}", trigram, count);
                        }

                        break;
                    }
                }
            }
        }
    }

    Ok(())
}
