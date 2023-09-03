use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use croissantine::available_docids_iter::AvailableDocIds;
use croissantine::database::Database;
use croissantine::encode_trigram;
use croissantine::text::cleanup_chars;
use croissantine::text::trigrams::TriGrams;
use flate2::read::MultiGzDecoder;
use heed::EnvOpenOptions;
use httparse::{Response, Status, EMPTY_HEADER};
use roaring::RoaringTreemap;
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
    fs::create_dir_all(&database_path)?;
    let database = Database::open_or_create(options, database_path)?;

    let mut wtxn = database.write_txn()?;
    let mut all_docids = database.all_docids(&wtxn)?;
    let mut available_docids = AvailableDocIds::new(&all_docids);
    let file =
        File::open(&file_path).with_context(|| format!("while opening {}", file_path.display()))?;
    let reader = BufReader::new(file);
    let uncompressed = BufReader::new(MultiGzDecoder::new(reader));
    let warc = warc::WarcReader::new(uncompressed);

    let mut title_ngrams_docids = HashMap::<_, RoaringTreemap>::new();
    let mut content_ngrams_docids = HashMap::<_, RoaringTreemap>::new();

    for result in warc.iter_records() {
        let record = result?;
        if record.warc_type() == &RecordType::Response {
            if let Some(uri) = record.header(WarcHeader::TargetURI) {
                let url = Url::parse(&uri)?;
                let docid = available_docids.next().unwrap();
                all_docids.insert(docid);
                println!("{}", url);

                database.docid_uri.put(&mut wtxn, &docid, url.as_str())?;

                let mut headers = [EMPTY_HEADER; 64];
                let mut req = Response::new(&mut headers);
                let http_body = record.body();
                if let Ok(Status::Complete(size)) = req.parse(http_body) {
                    let html_body = &http_body[size..];
                    let product = readability::extractor::extract(&mut &html_body[..], &url)?;

                    for trigram in TriGrams::new(cleanup_chars(product.title.chars())) {
                        title_ngrams_docids.entry(trigram).or_default().insert(docid);
                    }

                    for trigram in TriGrams::new(cleanup_chars(product.text.chars())) {
                        content_ngrams_docids.entry(trigram).or_default().insert(docid);
                    }
                }
            }
        }
    }

    // Write everything into LMDB

    database.put_all_docids(&mut wtxn, &all_docids)?;

    let mut key = String::new();
    for (trigram, bitmap) in title_ngrams_docids {
        let before = database
            .title_ngrams_docids
            .get(&wtxn, encode_trigram(&mut key, trigram))?
            .unwrap_or_default();
        let bitmap = bitmap | before;
        database.title_ngrams_docids.put(&mut wtxn, &key, &bitmap)?;
    }

    for (trigram, bitmap) in content_ngrams_docids {
        let before = database
            .content_ngrams_docids
            .get(&wtxn, encode_trigram(&mut key, trigram))?
            .unwrap_or_default();
        let bitmap = bitmap | before;
        database.content_ngrams_docids.put(&mut wtxn, &key, &bitmap)?;
    }

    wtxn.commit()?;

    Ok(())
}
