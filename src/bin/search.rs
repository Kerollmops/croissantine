use std::io;
use std::path::PathBuf;

use clap::Parser;
use croissantine::database::Database;
use heed::EnvOpenOptions;

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
    let database = Database::open_or_create(options, database_path);

    let lines = io::stdin().lines();
    for line in lines {
        println!("got a line: {}", line.unwrap());
    }

    Ok(())
}
