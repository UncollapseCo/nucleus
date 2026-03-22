use std::sync::Arc;

use bytes::{Buf, Bytes};

use crate::NucleusNode;

use super::{DeserializationError, Message, Serializable};

#[diagnostic::do_not_recommend]
impl<T: prost::Message + Default + 'static> Serializable for T {
    // #[instrument(skip(self))]
    fn serialize(&self) -> Bytes {
        self.encode_to_vec().into()
    }

    fn serialize_into<B>(&self, buf: &mut B)
    where
        B: bytes::BufMut,
    {
        let required = self.encoded_len();
        let remaining = buf.remaining_mut();

        if required > remaining {
            panic!(
                "Buffer does not have sufficient capacity: required {}, remaining {}",
                required, remaining
            );
        }

        self.encode_raw(buf);
    }

    // #[instrument(skip(data))]
    fn deserialize<B>(data: B, _nucleus: Option<&NucleusNode>) -> Result<Self, DeserializationError>
    where
        B: Buf,
    {
        T::decode(data).map_err(|e| {
            error!("Failed to decode a protobuf message: {:?}", e);
            e.into()
        })
    }
}

impl<T> Message for Arc<T>
where
    T: Message,
{
    type Result = T::Result;

    fn name() -> &'static str {
        T::name()
    }
}

impl<T> Message for Box<T>
where
    T: Message,
{
    type Result = T::Result;

    fn name() -> &'static str {
        T::name()
    }
}
