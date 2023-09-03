use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Instant;
use std::{fs, io};

use clap::Parser;
use croissantine::database::Database;
use croissantine::encode_trigram;
use croissantine::text::cleanup_chars;
use croissantine::text::trigrams::TriGrams;
use heed::EnvOpenOptions;
use roaring::{MultiOps, RoaringBitmap};

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
    options.map_size(100 * 1024 * 1024 * 1024); // 100GiB
    fs::create_dir_all(&database_path)?;
    let database = Database::open_or_create(options, database_path)?;

    let lines = io::stdin().lines();
    for result in lines {
        let line = result?;
        let before = Instant::now();
        let trigrams: Vec<_> = TriGrams::new(cleanup_chars(line.chars())).collect();
        let trigrams = &trigrams[1..trigrams.len() - 1];
        let rtxn = database.read_txn()?;
        let mut title_trigram_bitmaps = Vec::new();
        let mut content_trigram_bitmaps = Vec::new();
        let mut key = String::new();
        for trigram in trigrams {
            let key = encode_trigram(&mut key, *trigram);
            if let Some(bitmap) = database.title_ngrams_docids.get(&rtxn, key)? {
                title_trigram_bitmaps.push(bitmap);
            }
            if let Some(bitmap) = database.content_ngrams_docids.get(&rtxn, key)? {
                content_trigram_bitmaps.push(bitmap);
            }
        }

        let title_bitmap = title_trigram_bitmaps.intersection();
        let mut content_bitmap = content_trigram_bitmaps.intersection();
        content_bitmap -= &title_bitmap;

        for (i, docid) in title_bitmap.into_iter().chain(content_bitmap).take(20).enumerate() {
            if let Some(url) = database.docid_uri.get(&rtxn, &docid)? {
                println!("{i:>3}. {url}");
            }
        }

        eprintln!("Searching for `{}` took {:.02?}", line, before.elapsed());
    }

    Ok(())
}
