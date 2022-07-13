use std::env;
use futures_util::{StreamExt};
use tokio::io::{AsyncReadExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use redis::{
    aio::MultiplexedConnection,
};

#[tokio::main]
async fn main() -> Result<(), Box<(dyn std::error::Error + 'static)>> {
    let client = redis::Client::open("redis://172.17.0.2/").unwrap();
    let con = client.get_multiplexed_tokio_connection().await?;
    let connect_addr =
        env::args().nth(1).unwrap_or_else(|| panic!("this program requires at least one argument"));

    let url = url::Url::parse(&connect_addr).unwrap();

    let (stdin_tx, _) = futures_channel::mpsc::unbounded();
    tokio::spawn(read_stdin(stdin_tx));

    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    println!("WebSocket handshake has been successfully completed");

    let (write, read) = ws_stream.split();

    let _ = read.fold(write, |write, m| async {
        match m  {
            // Error here...
            // tungstenite::error::Error
            Err(e) => { 
                println!("error {:?}", e);
                ()
            },
            Ok(message) => {
                let _ = send_to_redis_stream(message, &con).await;
                ()
            },
        };
        write
    }).await;
    Ok(())
}

async fn send_to_redis_stream(
    message: Message,
    con: &MultiplexedConnection,
) -> Result<(), Box<(dyn std::error::Error + 'static)>> {
    let mut con = con.clone();
    println!("Message {}", message);
    redis::cmd("XADD").arg("mystream").arg("*").arg(&["data", &message.to_string()[..] ]).query_async(&mut con).await?;
    Ok(())
}

async fn read_stdin(tx: futures_channel::mpsc::UnboundedSender<Message>) {
    let mut stdin = tokio::io::stdin();
    loop {
        let mut buf = vec![0; 1024];
        let n = match stdin.read(&mut buf).await {
            Err(_) | Ok(0) => break,
            Ok(n) => n,
        };
        buf.truncate(n);
        tx.unbounded_send(Message::binary(buf)).unwrap();
    }
}

/* fn print_type_of<T>(_: &T) {
    println!("Type {}", std::any::type_name::<T>())
} */