use nom::{
    IResult,
    number::complete::{be_i16, be_i32},
};

use crate::kafka_request::KafkaRequest;

pub fn parse_kafka_frame(input: &[u8]) -> IResult<&[u8], KafkaRequest> {
    let (input, request_api_key) = be_i16(input)?;
    let (input, request_api_version) = be_i16(input)?;
    let (input, correlation_id) = be_i32(input)?;

    Ok((
        input,
        KafkaRequest {
            request_api_key,
            request_api_version,
            correlation_id,
        },
    ))
}
