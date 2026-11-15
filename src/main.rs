#![allow(unused_imports)]
use anyhow::Result;
use bytes::{BufMut, BytesMut};
use futures_util::{SinkExt, StreamExt};
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

    let codec = LengthDelimitedCodec::builder()
        .length_field_length(4)
        .length_field_offset(0)
        .new_codec();

    // In future, we will want to have framed reads.
    // For now ignore the input and return a result
    //let mut reader = FramedRead::new(raw_reader, RespCodec {});

    let mut writer = FramedWrite::new(raw_writer, codec);

    let mut payload = BytesMut::with_capacity(4);
    // Hardcoded correlation ID 7
    payload.put_i32(7);

    writer.send(payload.freeze()).await?;
    Ok(())
}
