use std::collections::HashMap;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use std::{fs, io};

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
use rayon::prelude::{ParallelBridge, ParallelIterator};
use roaring::RoaringTreemap;
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

        let url = task.url();
        let request = ureq::get(url.as_str()).call()?;
        let mut reader = request.into_reader();
        let mut file = tempfile::tempfile()?;
        let before = Instant::now();
        let length = io::copy(&mut reader, &mut file)?;
        file.seek(SeekFrom::Start(0))?;
        let reader = BufReader::new(file);

        match task {
            Task::WarcUrlPaths(_) => {
                eprintln!(
                    "Fetched the WARC path file ({length} bytes) in {:.02?}",
                    before.elapsed()
                );
                // The WarcUrls have always incrementing ids while the WarcUrlPaths
                // always decrementing ones. We always processes tasks from the
                // smallest to the biggests.
                let uncompressed = BufReader::new(GzDecoder::new(reader));
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
                eprintln!("Fetched the WARC file ({length} bytes) in {:.02?}", before.elapsed());
                let before = Instant::now();
                let uncompressed = BufReader::new(MultiGzDecoder::new(reader));
                let warc = warc::WarcReader::new(uncompressed);

                let mut all_docids = database.all_docids(&wtxn)?;
                let available_docids = AvailableDocIds::new(&all_docids);

                let IndexingOutput { title_ngrams_docids, content_ngrams_docids, docids, urls } =
                    warc.iter_records()
                        .zip(available_docids)
                        .par_bridge()
                        .map(|(result, docid)| {
                            let record = result.unwrap();
                            let uri =
                                match (record.warc_type(), record.header(WarcHeader::TargetURI)) {
                                    (RecordType::Response, Some(uri)) => uri,
                                    _ => return None,
                                };

                            let url = Url::parse(&uri).unwrap();
                            let mut title_ngrams_docids = HashMap::<_, RoaringTreemap>::new();
                            let mut content_ngrams_docids = HashMap::<_, RoaringTreemap>::new();

                            let mut headers = [EMPTY_HEADER; 64];
                            let mut req = Response::new(&mut headers);
                            let http_body = record.body();
                            if let Ok(Status::Complete(size)) = req.parse(http_body) {
                                let html_body = &http_body[size..];
                                let product =
                                    readability::extractor::extract(&mut &html_body[..], &url)
                                        .unwrap();

                                for trigram in TriGrams::new(cleanup_chars(product.title.chars())) {
                                    title_ngrams_docids.entry(trigram).or_default().insert(docid);
                                }

                                for trigram in TriGrams::new(cleanup_chars(product.text.chars())) {
                                    content_ngrams_docids.entry(trigram).or_default().insert(docid);
                                }
                            }

                            Some(IndexingOutput {
                                title_ngrams_docids,
                                content_ngrams_docids,
                                docids: RoaringTreemap::from_iter([docid]),
                                urls: vec![url],
                            })
                        })
                        .flatten()
                        .reduce(IndexingOutput::default, IndexingOutput::merge);

                let count = docids.len();

                eprintln!(
                    "{count} documents seen in {:.02?}, will commit soon...",
                    before.elapsed()
                );
                
                let before_commit = Instant::now();

                for (docid, url) in docids.iter().zip(urls) {
                    database.docid_uri.put(&mut wtxn, &docid, url.as_str())?;
                }

                all_docids |= docids;

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

                eprintln!("Writing into the database took {:.02?}", before_commit.elapsed());
                eprintln!("Processed {count} documents in {:.02?}, committed!", before.elapsed());
            }
        }
    }
}

#[derive(Debug, Default)]
struct IndexingOutput {
    title_ngrams_docids: HashMap<[char; 3], RoaringTreemap>,
    content_ngrams_docids: HashMap<[char; 3], RoaringTreemap>,
    docids: RoaringTreemap,
    urls: Vec<Url>,
}

impl IndexingOutput {
    fn merge(mut self, other: Self) -> Self {
        let IndexingOutput { title_ngrams_docids, content_ngrams_docids, docids, mut urls } = other;

        for (ngram, docids) in title_ngrams_docids {
            *self.title_ngrams_docids.entry(ngram).or_default() |= docids;
        }

        for (ngram, docids) in content_ngrams_docids {
            *self.content_ngrams_docids.entry(ngram).or_default() |= docids;
        }

        self.urls.append(&mut urls);
        self.docids |= docids;

        self
    }
}
