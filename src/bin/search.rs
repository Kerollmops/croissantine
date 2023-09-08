use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use std::{fs, include_bytes};

use askama::Template;
use axum::extract::{Query, State};
use axum::http::{header, Response};
use axum::response::{IntoResponse, Redirect};
use axum::routing::get;
use axum::{Form, Router};
use axum_auth::AuthBasic;
use clap::Parser;
use croissantine::database::Database;
use croissantine::task::Task;
use croissantine::text::cleanup_chars;
use croissantine::text::trigrams::TriGrams;
use croissantine::{encode_trigram, DATABASE_MAX_SIZE};
use heed::EnvOpenOptions;
use roaring::MultiOps;
use serde::Deserialize;
use url::Url;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Options {
    /// The binding to listen to.
    #[arg(long, default_value = "0.0.0.0:3000")]
    listen: String,

    /// The database path where the indexed data is stored.
    #[arg(long, default_value = "croissantine.db")]
    database_path: PathBuf,
}

struct AppState {
    database: Database,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Options { listen, database_path } = Options::parse();

    let mut options = EnvOpenOptions::new();
    options.map_size(DATABASE_MAX_SIZE);
    fs::create_dir_all(&database_path)?;
    let database = Database::open_or_create(options, database_path)?;
    let app_state = Arc::new(AppState { database });

    // our router
    let app = Router::new()
        .route("/", get(welcome))
        .route("/search", get(search))
        .route("/indexer", get(indexer))
        .route("/register-warc", get(register_warc))
        .route("/about", get(about))
        .route("/redirect", get(redirect))
        .route("/assets/images/croissantine-logo.svg", get(assets_images_logo))
        .with_state(app_state);

    // run it with hyper on localhost:3000
    let addr = listen.parse().unwrap();
    axum::Server::bind(&addr).serve(app.into_make_service()).await?;

    Ok(())
}

#[derive(Template)]
#[template(path = "welcome.html")]
struct WelcomeTemplate {
    total_count: u64,
}

async fn welcome(State(state): State<Arc<AppState>>) -> WelcomeTemplate {
    let rtxn = state.database.read_txn().unwrap();
    let all_docids = state.database.all_docids(&rtxn).unwrap();
    WelcomeTemplate { total_count: all_docids.len() }
}

#[derive(Template)]
#[template(path = "results.html")]
struct ResultsTemplate {
    time_taken: String,
    count: u64,
    query: String,
    results: Vec<Result>,
}

struct Result {
    link: String,
    title: String,
}

async fn search(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let database = &state.database;
    let query = match params.get("query").cloned() {
        Some(query) if !query.is_empty() => query,
        _ => return Box::new(Redirect::temporary("/")).into_response(),
    };

    let before = Instant::now();
    let trigrams: Vec<_> = TriGrams::new(cleanup_chars(query.chars())).collect();
    let trigrams = &trigrams[1..trigrams.len() - 1];
    let rtxn = database.read_txn().unwrap();
    let mut title_trigram_bitmaps = Vec::new();
    let mut content_trigram_bitmaps = Vec::new();
    let mut key = String::new();
    for trigram in trigrams {
        let key = encode_trigram(&mut key, *trigram);
        if let Some(bitmap) = database.title_ngrams_docids.get(&rtxn, key).unwrap() {
            title_trigram_bitmaps.push(bitmap);
        }
        if let Some(bitmap) = database.content_ngrams_docids.get(&rtxn, key).unwrap() {
            content_trigram_bitmaps.push(bitmap);
        }
    }

    let title_bitmap = title_trigram_bitmaps.intersection();
    let mut content_bitmap = content_trigram_bitmaps.intersection();
    content_bitmap -= &title_bitmap;
    let count = title_bitmap.union_len(&content_bitmap);

    let mut results = Vec::new();
    for (i, docid) in title_bitmap.into_iter().chain(content_bitmap).take(20).enumerate() {
        if let Some(url) = database.docid_uri.get(&rtxn, &docid).unwrap() {
            let title = url.to_string();
            let link = generate_redirect_url(&url, i, &query);
            results.push(Result { link, title });
        }
    }

    eprintln!("Searching for `{}` took {:.02?}", query, before.elapsed());
    Box::new(ResultsTemplate {
        time_taken: format!("{:.02?}", before.elapsed()),
        count,
        query,
        results,
    })
    .into_response()
}

async fn redirect(Query(params): Query<HashMap<String, String>>) -> Redirect {
    match params.get("url") {
        Some(url) => Redirect::temporary(url),
        None => Redirect::temporary("/"),
    }
}

async fn about() -> Redirect {
    Redirect::temporary("https://github.com/Kerollmops/croissantine")
}

#[derive(Template)]
#[template(path = "indexer.html")]
struct IndexerTemplate {
    tasks: Vec<Url>,
}

async fn indexer(
    AuthBasic((id, password)): AuthBasic,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    if id == "admin" && password.map_or(false, |p| p == "53gb78855qdqsdlopnert") {
        let database = &state.database;
        let rtxn = database.read_txn().unwrap();
        let tasks = database
            .enqueued
            .iter(&rtxn)
            .unwrap()
            .flat_map(|r| r.ok().map(|(_, task)| task.url().clone()))
            .collect();
        IndexerTemplate { tasks }.into_response()
    } else {
        Redirect::temporary("/").into_response()
    }
}

#[derive(Deserialize)]
struct WarcIdRegistering {
    #[serde(rename = "warcId")]
    warc_id: String,
}

async fn register_warc(
    AuthBasic((id, password)): AuthBasic,
    State(state): State<Arc<AppState>>,
    Form(WarcIdRegistering { warc_id }): Form<WarcIdRegistering>,
) -> Redirect {
    if id == "admin" && password.map_or(false, |p| p == "53gb78855qdqsdlopnert") {
        let database = &state.database;
        let mut wtxn = database.write_txn().unwrap();
        let task_id = database.available_reverse_enqueued_id(&wtxn).unwrap();
        let url = format!("https://data.commoncrawl.org/crawl-data/{warc_id}/warc.paths.gz");
        let task = Task::WarcUrlPaths(Url::parse(&url).unwrap());
        database.enqueued.put(&mut wtxn, &task_id, &task).unwrap();
        wtxn.commit().unwrap();
    }

    Redirect::temporary("/indexer")
}

async fn assets_images_logo() -> impl IntoResponse {
    let bytes = include_bytes!("../../assets/images/croissantine-logo.svg");
    ([(header::CONTENT_TYPE, "image/svg+xml")], bytes)
}

/// Generates a route that'll redirect to the link but we can have more info
/// on the quality of the results for a given query.
fn generate_redirect_url(url: &str, index: usize, query: &str) -> String {
    format!(
        "/redirect?url={}&index={}&query={}",
        urlencoding::encode(url),
        index,
        urlencoding::encode(query)
    )
}
