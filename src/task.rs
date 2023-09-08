use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Deserialize, Serialize)]
pub enum Task {
    WarcUrlPaths(Url),
    WarcUrl(Url),
}

impl Task {
    pub fn url(&self) -> &Url {
        match self {
            Self::WarcUrlPaths(url) => url,
            Self::WarcUrl(url) => url,
        }
    }
}
