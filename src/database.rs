use std::path::Path;

use heed::byteorder::BE;
use heed::types::{Str, U64};
use heed::{Env, EnvOpenOptions, RoTxn, RwTxn, Unspecified};

use crate::roaring64_codec::Roaring64Codec;

pub struct Database {
    env: Env,
    main: heed::Database<Unspecified, Unspecified>,
    pub ngrams_docids: heed::Database<Str, Roaring64Codec>,
    pub docid_uri: heed::Database<U64<BE>, Str>,
}

impl Database {
    pub fn open_or_create(
        mut options: EnvOpenOptions,
        path: impl AsRef<Path>,
    ) -> heed::Result<Database> {
        let env = options.max_dbs(10).open(path)?;
        let mut wtxn = env.write_txn()?;
        let main = env.create_database(&mut wtxn, None)?;
        let ngrams_docids = env.create_database(&mut wtxn, Some("ngrams-docids"))?;
        let docid_uri = env.create_database(&mut wtxn, Some("docid-uri"))?;
        wtxn.commit()?;

        Ok(Database { env, main, ngrams_docids, docid_uri })
    }

    pub fn read_txn(&self) -> heed::Result<RoTxn> {
        self.env.read_txn()
    }

    pub fn write_txn(&self) -> heed::Result<RwTxn> {
        self.env.write_txn()
    }
}
