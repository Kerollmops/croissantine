use std::borrow::Cow;

use heed::BoxedError;
use roaring::RoaringTreemap;

pub struct RoaringTreemapCodec;

impl heed::BytesDecode<'_> for RoaringTreemapCodec {
    type DItem = RoaringTreemap;

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        RoaringTreemap::deserialize_unchecked_from(bytes).map_err(Into::into)
    }
}

impl heed::BytesEncode<'_> for RoaringTreemapCodec {
    type EItem = RoaringTreemap;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<[u8]>, BoxedError> {
        let mut bytes = Vec::with_capacity(item.serialized_size());
        item.serialize_into(&mut bytes).map_err(BoxedError::from)?;
        Ok(Cow::Owned(bytes))
    }
}
