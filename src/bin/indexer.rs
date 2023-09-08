use std::collections::HashMap;
use std::fmt::format;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::Duration;
use std::{fs, io};

use anyhow::Context;
use clap::Parser;
use croissantine::available_docids_iter::AvailableDocIds;
use croissantine::database::Database;
use croissantine::task::Task;
use croissantine::text::cleanup_chars;
use croissantine::text::trigrams::TriGrams;
use croissantine::{encode_trigram, DATABASE_MAX_SIZE};
use flate2::bufread::GzDecoder;
use flate2::read::MultiGzDecoder;
use heed::EnvOpenOptions;
use httparse::{Response, Status, EMPTY_HEADER};
use roaring::RoaringTreemap;
use tempfile::tempfile;
use url::Url;
use warc::{RecordType, WarcHeader};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Options {
    /// The database path where the indexed data is stored.
    #[arg(long, default_value = "croissantine.db")]
    database_path: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let Options { database_path } = Options::parse();

    let mut options = EnvOpenOptions::new();
    options.map_size(DATABASE_MAX_SIZE);
    fs::create_dir_all(&database_path)?;
    let database = Database::open_or_create(options, database_path)?;

    loop {
        let mut wtxn = database.write_txn()?;
        let mut tasks = database.enqueued.iter(&wtxn)?;
        let (task_id, task) = match tasks.next().transpose()? {
            Some(entry) => entry,
            None => {
                drop(tasks);
                wtxn.abort();
                eprintln!("No new task found to process, waiting 5h...");
                std::thread::sleep(Duration::from_secs(5 * 60 * 60)); // 5 hours
                continue;
            }
        };

        drop(tasks);

        let mut all_docids = database.all_docids(&wtxn)?;
        let mut available_docids = AvailableDocIds::new(&all_docids);

        let url = task.url();
        let request = ureq::get(url.as_str()).call()?;
        let mut reader = request.into_reader();
        let mut file = tempfile::tempfile()?;
        let length = io::copy(&mut reader, &mut file)?;
        let reader = BufReader::new(file);
        let uncompressed = BufReader::new(GzDecoder::new(reader));

        match task {
            Task::WarcUrlPaths(_) => {
                eprintln!("Fetched the warc path file ({length} bytes)");
                // The WarcUrls have always incrementing ids while the WarcUrlPaths
                // always decrementing ones. We always processes tasks from the
                // smallest to the biggests.
                for (i, result) in uncompressed.lines().enumerate() {
                    let path = result?;
                    if !path.is_empty() {
                        let url = Url::parse(&format!("https://data.commoncrawl.org/{}", path))?;
                        let key: u32 = i.try_into().unwrap();
                        database.enqueued.put(&mut wtxn, &key, &Task::WarcUrl(url))?;
                    }
                }

                // Remove the tasks now
                database.enqueued.delete(&mut wtxn, &task_id)?;
                wtxn.commit()?;
            }
            // The CommonCrawl Gzipped WARC file to analyze
            Task::WarcUrl(_) => {
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
                            eprintln!("{}", url);

                            database.docid_uri.put(&mut wtxn, &docid, url.as_str())?;

                            let mut headers = [EMPTY_HEADER; 64];
                            let mut req = Response::new(&mut headers);
                            let http_body = record.body();
                            if let Ok(Status::Complete(size)) = req.parse(http_body) {
                                let html_body = &http_body[size..];
                                let product =
                                    readability::extractor::extract(&mut &html_body[..], &url)?;

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

                // Remove the tasks now
                database.enqueued.delete(&mut wtxn, &task_id)?;
                wtxn.commit()?;
            }
        }
    }
}
