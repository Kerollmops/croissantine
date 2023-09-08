# Croissantine

An open source french search engine experiment using the Common Crawl WARC files for now.

<https://index.commoncrawl.org/collinfo.json>

<https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-23/warc.paths.gz>

## Deploy on the already running instance

```bash
git pull --prune --rebase
cargo install --path .
systemctl restart croissantine
```
