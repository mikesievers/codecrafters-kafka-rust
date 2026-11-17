use std::io;

use bytes::BufMut;
use tokio_util::codec::Encoder;

use crate::kafka_response::KafkaResponse;

pub trait KafkaEncodable {
    fn encode_body(&self, buf: &mut impl BufMut);
}

pub struct KafkaCodec;

impl Encoder<KafkaResponse> for KafkaCodec {
    type Error = io::Error;

    fn encode(
        &mut self,
        item: KafkaResponse,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        let placeholder_length_pos = dst.len();

        // This will be replaced by the message length
        dst.put_i32(0);

        // Write body to buffer
        match item {
            KafkaResponse::ApiVersions(api_versions_response) => {
                api_versions_response.encode_body(dst)
            }
        }

        // Compute and store length
        let body_length = (dst.len() - placeholder_length_pos - 4) as i32;

        // Overwrite the placeholder with the body length
        dst[placeholder_length_pos..placeholder_length_pos + 4]
            .copy_from_slice(&body_length.to_be_bytes());

        Ok(())
    }
}
