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

#[cfg(test)]
mod tests {

    // Client requirements.
    use std::env;

    use futures_util::{future, pin_mut, StreamExt};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
    static ADDRESS: &str = "127.0.0.1:12345";
    static WS_PREFIX: &'static str = "ws://";

    // Server requirements
    use std::{
        collections::HashMap,
        // env,
        io::Error as IoError,
        net::SocketAddr,
        sync::{Arc, Mutex},
    };
    
    use futures_channel::mpsc::{unbounded, UnboundedSender};
    // use futures_util::{future, pin_mut, stream::TryStreamExt, StreamExt};
    use futures_util::{stream::TryStreamExt};
    
    use tokio::net::{TcpListener, TcpStream};
    // use tungstenite::protocol::Message;
    
    type Tx = UnboundedSender<Message>;
    type PeerMap = Arc<Mutex<HashMap<SocketAddr, Tx>>>;
    
    #[test]
    fn all() {
        let WS_ADDRESS= WS_PREFIX.to_owned() + ADDRESS;
        let message = "assert message";
        println!("Starting test");
        start_server(ADDRESS);
        start_client(WS_ADDRESS.as_str());
        println!("test completed.")
    }

    // Client needs a server
    #[tokio::main]
    async fn start_client(ws_address:&str) {
        println!("client");
        use super::*;
        let connect_addr =
            env::args().nth(1).unwrap_or_else(|| panic!("this program requires at least one argument"));
    
        let url = url::Url::parse(&ws_address).unwrap();
    
        let (stdin_tx, stdin_rx) = futures_channel::mpsc::unbounded();
        tokio::spawn(read_stdin(stdin_tx));
    
        let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
        println!("WebSocket handshake has been successfully completed");
    
        let (write, read) = ws_stream.split();
    
        let stdin_to_ws = stdin_rx.map(Ok).forward(write);
        let ws_to_stdout = {
            read.for_each(|message| async {
                let data = message.unwrap().into_data();
                tokio::io::stdout().write_all(&data).await.unwrap();
            })
        };
    
        pin_mut!(stdin_to_ws, ws_to_stdout);
        future::select(stdin_to_ws, ws_to_stdout).await;
    }
    
    // Our helper method which will read data from stdin and send it along the
    // sender provided.
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
    
    async fn handle_connection(peer_map: PeerMap, raw_stream: TcpStream, addr: SocketAddr) {
        println!("Incoming TCP connection from: {}", addr);
    
        let ws_stream = tokio_tungstenite::accept_async(raw_stream)
            .await
            .expect("Error during the websocket handshake occurred");
        println!("WebSocket connection established: {}", addr);
    
        // Insert the write part of this peer to the peer map.
        let (tx, rx) = unbounded();
        peer_map.lock().unwrap().insert(addr, tx);
    
        let (outgoing, incoming) = ws_stream.split();
    
        let broadcast_incoming = incoming.try_for_each(|msg| {
            println!("Received a message from {}: {}", addr, msg.to_text().unwrap());
            let peers = peer_map.lock().unwrap();
    
            // We want to broadcast the message to everyone except ourselves.
            let broadcast_recipients =
                peers.iter().filter(|(peer_addr, _)| peer_addr != &&addr).map(|(_, ws_sink)| ws_sink);
    
            for recp in broadcast_recipients {
                recp.unbounded_send(msg.clone()).unwrap();
            }
    
            future::ok(())
        });
    
        let receive_from_others = rx.map(Ok).forward(outgoing);
    
        pin_mut!(broadcast_incoming, receive_from_others);
        future::select(broadcast_incoming, receive_from_others).await;
    
        println!("{} disconnected", &addr);
        peer_map.lock().unwrap().remove(&addr);
    }
    
    #[tokio::main]
    async fn start_server(ws_address: &str) -> Result<(), IoError> {
        // let addr = env::args().nth(1).unwrap_or_else(|| "127.0.0.1:8080".to_string());
        let addr = ws_address;
    
        let state = PeerMap::new(Mutex::new(HashMap::new()));
    
        // Create the event loop and TCP listener we'll accept connections on.
        let try_socket = TcpListener::bind(&addr).await;
        let listener = try_socket.expect("Failed to bind");
        println!("Listening on: {}", addr);
    
        // Let's spawn the handling of each connection in a separate task.
        while let Ok((stream, addr)) = listener.accept().await {
            tokio::spawn(handle_connection(state.clone(), stream, addr));
        }
    
        Ok(())
    }
}


/* fn print_type_of<T>(_: &T) {
    println!("Type {}", std::any::type_name::<T>())
} */