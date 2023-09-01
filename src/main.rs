use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use flate2::read::MultiGzDecoder;
use warc::WarcHeader;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Options {
    /// The CommonCrawl Gzipped WARC file to analyze
    #[arg(value_name = "FILE")]
    file: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let Options { file } = Options::parse();

    let file = File::open(&file).with_context(|| format!("while opening {}", file.display()))?;
    let reader = BufReader::new(file);
    let uncompressed = BufReader::new(MultiGzDecoder::new(reader));
    let mut warc = warc::WarcReader::new(uncompressed);

    let mut iter = warc.stream_records();
    while let Some(result) = iter.next_item() {
        let record = result?;
        if let Some(uri) = record.header(WarcHeader::TargetURI) {
            println!("{}", uri);
        }
    }

    Ok(())
}
