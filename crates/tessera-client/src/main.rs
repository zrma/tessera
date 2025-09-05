use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use std::path::PathBuf;
use tessera_core::{ClientMsg, EntityId, Position, ServerMsg, encode_frame};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Parser, Debug)]
#[command(name = "tessera-client", about = "Simple test client for Tessera V0")]
struct Cli {
    /// Gateway address (host:port)
    #[arg(long, default_value = "127.0.0.1:4000")]
    addr: String,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Send Ping and print Pong
    Ping {
        #[arg(long, default_value_t = 1)]
        ts: u64,
    },
    /// Join the world with actor id at position
    Join {
        #[arg(long)]
        actor: u64,
        #[arg(long, default_value_t = 0.0)]
        x: f32,
        #[arg(long, default_value_t = 0.0)]
        y: f32,
    },
    /// Move an actor by delta and print Delta
    Move {
        #[arg(long)]
        actor: u64,
        #[arg(long)]
        dx: f32,
        #[arg(long)]
        dy: f32,
    },
    /// Demo: join + a few moves
    Demo {
        #[arg(long, default_value_t = 1)]
        actor: u64,
    },
    /// Interactive REPL: type ping/join/move/quit
    Repl {
        /// Optional actor id to default in join/move
        #[arg(long)]
        actor: Option<u64>,
    },
    /// Run commands from a script file (one per line)
    Script {
        /// Path to script file
        path: PathBuf,
        /// Optional default actor id
        #[arg(long)]
        actor: Option<u64>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let addr: SocketAddr = cli
        .addr
        .parse()
        .with_context(|| format!("invalid addr: {}", cli.addr))?;

    let mut stream = TcpStream::connect(addr)
        .await
        .with_context(|| format!("connect to {}", addr))?;

    match cli.cmd {
        Cmd::Ping { ts } => {
            send(&mut stream, ClientMsg::Ping { ts }).await?;
            let msg: ServerMsg = recv(&mut stream).await?;
            println!("<- {:?}", msg);
        }
        Cmd::Join { actor, x, y } => {
            let msg = ClientMsg::Join {
                actor: EntityId(actor),
                pos: Position { x, y },
            };
            send(&mut stream, msg).await?;
            let reply: ServerMsg = recv(&mut stream).await?;
            println!("<- {:?}", reply);
        }
        Cmd::Move { actor, dx, dy } => {
            let msg = ClientMsg::Move {
                actor: EntityId(actor),
                dx,
                dy,
            };
            send(&mut stream, msg).await?;
            let reply: ServerMsg = recv(&mut stream).await?;
            println!("<- {:?}", reply);
        }
        Cmd::Demo { actor } => {
            send(
                &mut stream,
                ClientMsg::Join {
                    actor: EntityId(actor),
                    pos: Position { x: 0.0, y: 0.0 },
                },
            )
            .await?;
            let _ = recv::<ServerMsg>(&mut stream).await?; // Snapshot
            for i in 0..3u32 {
                let msg = ClientMsg::Move {
                    actor: EntityId(actor),
                    dx: 1.0,
                    dy: 0.5,
                };
                send(&mut stream, msg).await?;
                let reply: ServerMsg = recv(&mut stream).await?;
                println!("step {} <- {:?}", i + 1, reply);
            }
        }
        Cmd::Repl { actor } => {
            println!(
                "REPL started. Commands: ping <ts> | join <actor?> <x> <y> | move <actor?> <dx> <dy> | quit"
            );
            use tokio::io::{AsyncBufReadExt, BufReader};
            let stdin = tokio::io::stdin();
            let mut reader = BufReader::new(stdin);
            let mut line = String::new();
            loop {
                line.clear();
                print!("> ");
                use std::io::Write;
                let _ = std::io::stdout().flush();
                if reader.read_line(&mut line).await? == 0 {
                    break;
                }
                if !handle_line(&line, &mut stream, actor).await? {
                    break;
                }
            }
        }
        Cmd::Script { path, actor } => {
            use tokio::io::{AsyncBufReadExt, BufReader};
            let f = tokio::fs::File::open(&path)
                .await
                .with_context(|| format!("open script: {}", path.display()))?;
            let mut reader = BufReader::new(f);
            let mut line = String::new();
            while reader.read_line(&mut line).await? != 0 {
                if !handle_line(&line, &mut stream, actor).await? {
                    break;
                }
                line.clear();
            }
        }
    }

    Ok(())
}

async fn send(stream: &mut TcpStream, msg: ClientMsg) -> Result<()> {
    let frame = encode_frame(&msg);
    stream.write_all(&frame).await?;
    Ok(())
}

async fn recv<T: for<'de> serde::Deserialize<'de>>(stream: &mut TcpStream) -> Result<T> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    let msg = serde_json::from_slice::<T>(&payload)?;
    Ok(msg)
}

async fn handle_line(line: &str, stream: &mut TcpStream, actor: Option<u64>) -> Result<bool> {
    let parts: Vec<_> = line.split_whitespace().collect();
    if parts.is_empty() {
        return Ok(true);
    }
    match parts[0] {
        "quit" | "exit" => return Ok(false),
        "ping" => {
            let ts: u64 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(1);
            send(stream, ClientMsg::Ping { ts }).await?;
            let r: ServerMsg = recv(stream).await?;
            println!("<- {:?}", r);
        }
        "join" => {
            let aid = parts
                .get(1)
                .and_then(|s| s.parse().ok())
                .or(actor)
                .unwrap_or(1);
            let x: f32 = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let y: f32 = parts.get(3).and_then(|s| s.parse().ok()).unwrap_or(0.0);
            send(
                stream,
                ClientMsg::Join {
                    actor: EntityId(aid),
                    pos: Position { x, y },
                },
            )
            .await?;
            let r: ServerMsg = recv(stream).await?;
            println!("<- {:?}", r);
        }
        "move" => {
            let aid = parts
                .get(1)
                .and_then(|s| s.parse().ok())
                .or(actor)
                .unwrap_or(1);
            let dx: f32 = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(1.0);
            let dy: f32 = parts.get(3).and_then(|s| s.parse().ok()).unwrap_or(0.0);
            send(
                stream,
                ClientMsg::Move {
                    actor: EntityId(aid),
                    dx,
                    dy,
                },
            )
            .await?;
            let r: ServerMsg = recv(stream).await?;
            println!("<- {:?}", r);
        }
        // Simple pacing helper: sleep <millis>
        "sleep" => {
            if let Some(ms) = parts.get(1).and_then(|s| s.parse::<u64>().ok()) {
                tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
            }
        }
        _ => {
            println!("unknown command");
        }
    }
    Ok(true)
}
