use std::borrow::Cow;

use heed::BoxedError;
use roaring::Roaring64;

pub struct Roaring64Codec;

impl heed::BytesDecode<'_> for Roaring64Codec {
    type DItem = Roaring64;

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        Roaring64::deserialize_unchecked_from(bytes).map_err(Into::into)
    }
}

impl heed::BytesEncode<'_> for Roaring64Codec {
    type EItem = Roaring64;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<[u8]>, BoxedError> {
        let mut bytes = Vec::with_capacity(item.serialized_size());
        item.serialize_into(&mut bytes).map_err(BoxedError::from)?;
        Ok(Cow::Owned(bytes))
    }
}
