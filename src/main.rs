#![allow(unused_imports)]
use anyhow::Result;
use bytes::{BufMut, BytesMut};
use futures_util::{SinkExt, StreamExt};
use nom::{
    IResult,
    number::complete::{be_i16, be_i32},
};
use tokio::{
    io::AsyncBufReadExt,
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

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
    let mut writer = FramedWrite::new(raw_writer, frame_codec);

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

        let mut payload = BytesMut::with_capacity(4);
        // Hardcoded correlation ID 7
        payload.put_i32(kafka_request.correlation_id);

        writer.send(payload.freeze()).await?;

        // For now, immediately break out of the loop
        break;
    }
    Ok(())
}

#[derive(Debug)]
struct KafkaRequest {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    // client_id: Option<String>,
}

fn parse_kafka_frame(input: &[u8]) -> IResult<&[u8], KafkaRequest> {
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
