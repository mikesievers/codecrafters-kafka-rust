// Remove later when it is clear whether the stream should loop or not
#![allow(clippy::never_loop)]

mod kafka_codec;
mod kafka_request;
mod kafka_response;
mod request_parser;

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::{
    kafka_codec::KafkaCodec,
    kafka_response::{ApiVersionsResponse, KafkaResponse},
    request_parser::parse_kafka_frame,
};

#[tokio::main]
async fn main() -> Result<()> {
    println!("Starting toy Kafka implementation.");

    let listener = TcpListener::bind("127.0.0.1:9092").await?;

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                println!("accepted new connection");

                tokio::spawn(handle_stream(stream));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn handle_stream(mut stream: TcpStream) -> Result<()> {
    let (raw_reader, raw_writer) = stream.split();

    let frame_codec = LengthDelimitedCodec::builder()
        .length_field_length(4)
        .big_endian()
        .length_adjustment(0)
        .num_skip(4) // Exclude length header from the result
        .new_codec();

    // In future, we will want to have framed reads.
    // For now ignore the input and return a result
    let mut reader = FramedRead::new(raw_reader, frame_codec.clone());
    let mut writer = FramedWrite::new(raw_writer, KafkaCodec {});

    while let Some(result) = reader.next().await {
        let kafka_request = match result {
            Ok(bytes_mut) => {
                let frame = bytes_mut.freeze();

                match parse_kafka_frame(&frame) {
                    Ok((_, request)) => {
                        println!("Received {:?}", request);
                        request
                    }
                    Err(e) => {
                        eprintln!("Error parsing frame: {:?}", e);
                        return Err(anyhow::anyhow!("Could not parse frame: {}", e));
                    }
                }
            }
            Err(e) => {
                eprintln!("Error reading frame: {:?}", e);
                return Err(anyhow::anyhow!("Error reading frame: {}", e));
            }
        };

        let error_code: i16 = match kafka_request.request_api_version {
            0..=4 => 0,
            _ => 35,
        };
        let reponse = KafkaResponse::ApiVersions(ApiVersionsResponse {
            correlation_id: kafka_request.correlation_id,
            error_code,
        });

        writer.send(reponse).await?;

        // For now, immediately break out of the loop
        break;
    }
    Ok(())
}
