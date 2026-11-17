use crate::kafka_codec::KafkaEncodable;

pub enum KafkaResponse {
    ApiVersions(ApiVersionsResponse),
}

pub struct ApiVersionsResponse {
    pub correlation_id: i32,
    pub error_code: i16,
}

impl KafkaEncodable for ApiVersionsResponse {
    fn encode_body(&self, buf: &mut impl bytes::BufMut) {
        buf.put_i32(self.correlation_id);
        buf.put_i16(self.error_code);
    }
}
