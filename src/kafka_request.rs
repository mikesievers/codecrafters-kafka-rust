#[derive(Debug)]
pub struct KafkaRequest {
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
    // client_id: Option<String>,
}
