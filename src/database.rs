use std::path::Path;

use heed::byteorder::BE;
use heed::types::{DecodeIgnore, SerdeJson, Str, U32, U64};
use heed::{Env, EnvOpenOptions, RoTxn, RwTxn, Unspecified};
use roaring::RoaringTreemap;

use crate::task::Task;
use crate::treemap_codec::RoaringTreemapCodec;

pub struct Database {
    env: Env,
    main: heed::Database<Unspecified, Unspecified>,
    pub title_ngrams_docids: heed::Database<Str, RoaringTreemapCodec>,
    pub content_ngrams_docids: heed::Database<Str, RoaringTreemapCodec>,
    pub docid_uri: heed::Database<U64<BE>, Str>,
    pub enqueued: heed::Database<U32<BE>, SerdeJson<Task>>,
}

impl Database {
    pub fn open_or_create(
        mut options: EnvOpenOptions,
        path: impl AsRef<Path>,
    ) -> heed::Result<Database> {
        let env = options.max_dbs(10).open(path)?;
        let mut wtxn = env.write_txn()?;
        let main = env.create_database(&mut wtxn, None)?;
        let title_ngrams_docids = env.create_database(&mut wtxn, Some("title-ngrams-docids"))?;
        let content_ngrams_docids =
            env.create_database(&mut wtxn, Some("content-ngrams-docids"))?;
        let docid_uri = env.create_database(&mut wtxn, Some("docid-uri"))?;
        let enqueued = env.create_database(&mut wtxn, Some("enqueued"))?;
        wtxn.commit()?;

        Ok(Database { env, main, title_ngrams_docids, content_ngrams_docids, docid_uri, enqueued })
    }

    pub fn read_txn(&self) -> heed::Result<RoTxn> {
        self.env.read_txn()
    }

    pub fn write_txn(&self) -> heed::Result<RwTxn> {
        self.env.write_txn()
    }

    pub fn all_docids(&self, rtxn: &RoTxn) -> heed::Result<RoaringTreemap> {
        self.main
            .remap_types::<Str, RoaringTreemapCodec>()
            .get(rtxn, "all-docids")
            .map(Option::unwrap_or_default)
    }

    pub fn put_all_docids(&self, wtxn: &mut RwTxn, bitmap: &RoaringTreemap) -> heed::Result<()> {
        self.main.remap_types::<Str, RoaringTreemapCodec>().put(wtxn, "all-docids", bitmap)
    }

    pub fn available_reverse_enqueued_id(&self, rtxn: &RoTxn) -> heed::Result<u32> {
        let iter = self.enqueued.rev_iter(rtxn)?.remap_data_type::<DecodeIgnore>();
        for (result, expected) in iter.zip((0..=u32::MAX).rev()) {
            let (task_id, _) = result?;
            if task_id != expected {
                return Ok(expected);
            }
        }
        Ok(u32::MAX)
    }
}
