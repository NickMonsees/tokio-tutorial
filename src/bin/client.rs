use tokio::sync::mpsc;
use tokio::sync::oneshot;
use mini_redis::client;
use bytes::Bytes;

#[tokio::main]
async fn main() {
    // Create a new channel with a capacity of at most 32
    // can clone multi-producer tx, cannot clone single-consumer rx
    let (tx, mut rx) = mpsc::channel(32);
    // cloning reference to tx lets us send from multiple tasks
    let tx2 = tx.clone();    

    let t1 = tokio::spawn(async move {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = Command::Get {
            key: "hello".to_string(),
            resp: resp_tx,
        };

        tx.send(cmd).await.unwrap();

        let res = resp_rx.await;
        println!("GOT = {:?}", res);
    });

    let t2 = tokio::spawn(async move {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = Command::Set {
            key: "foo".to_string(),
            val: "bar".into(),
            resp: resp_tx,
        };

        tx2.send(cmd).await.unwrap();

        let res = resp_rx.await;
        println!("GOT = {:?", res);
    });

    let manager = tokio::spawn(async move {
        let mut client = client::connect("127.0.0.1:6379").await.unwrap();
        while let Some(cmd) = rx.recv().await {
            use Command::*;

            match cmd {
                Get { key, resp } => {
                    client.get(&key).await;
                    // Ignore errors
                    let _ resp.send(res);
                }
                Set { key, val, resp } => {
                    client.set(&key, val).await;
                    // Ignore errors
                    let _ = resp.send(res);
                }
            }
        }
    });

    t1.await.unwrap();
    t2.await.unwrap();
    manager.await.unwrap();
}

#[derive(Debug)]
enum Command {
    Get {
        key: String,
        resp: Responder<Option<Bytes>>,
    },
    Set {
        key: String,
        val: Bytes,
        resp: Responder<()>,
    }
}

type Responder<t> = oneshot::Sender<mini_redis::Result<T>>;