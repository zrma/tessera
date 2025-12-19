use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use rustyline::{DefaultEditor, error::ReadlineError};
use std::path::PathBuf;
use tessera_core::{CellId, ClientMsg, EntityId, Envelope, Position, ServerMsg, encode_frame};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Parser, Debug)]
#[command(name = "tessera-client", about = "Simple test client for Tessera V0")]
struct Cli {
    /// Gateway address (host:port)
    #[arg(long, default_value = "127.0.0.1:4000")]
    addr: String,

    /// Target cell (defaults to world=0,cx=0,cy=0)
    #[arg(long, default_value_t = 0)]
    world: u32,
    #[arg(long, default_value_t = 0)]
    cx: i32,
    #[arg(long, default_value_t = 0)]
    cy: i32,
    /// Epoch to use in envelopes
    #[arg(long, default_value_t = 0)]
    epoch: u32,

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
    let addr = cli.addr.trim();
    let mut stream = TcpStream::connect(addr)
        .await
        .with_context(|| format!("connect to {}", addr))?;

    let cell = CellId::grid(cli.world, cli.cx, cli.cy);
    let epoch = cli.epoch;
    let mut seq: u64 = 0;

    match cli.cmd {
        Cmd::Ping { ts } => {
            send_env(&mut stream, cell, epoch, &mut seq, ClientMsg::Ping { ts }).await?;
            let env: Envelope<ServerMsg> = recv(&mut stream).await?;
            println!("<- {:?}", env);
        }
        Cmd::Join { actor, x, y } => {
            let msg = ClientMsg::Join {
                actor: EntityId(actor),
                pos: Position { x, y },
            };
            send_env(&mut stream, cell, epoch, &mut seq, msg).await?;
            let reply: Envelope<ServerMsg> = recv(&mut stream).await?;
            println!("<- {:?}", reply);
        }
        Cmd::Move { actor, dx, dy } => {
            let msg = ClientMsg::Move {
                actor: EntityId(actor),
                dx,
                dy,
            };
            send_env(&mut stream, cell, epoch, &mut seq, msg).await?;
            let reply: Envelope<ServerMsg> = recv(&mut stream).await?;
            println!("<- {:?}", reply);
        }
        Cmd::Demo { actor } => {
            send_env(
                &mut stream,
                cell,
                epoch,
                &mut seq,
                ClientMsg::Join {
                    actor: EntityId(actor),
                    pos: Position { x: 0.0, y: 0.0 },
                },
            )
            .await?;
            let _ = recv::<Envelope<ServerMsg>>(&mut stream).await?; // Snapshot
            for i in 0..3u32 {
                let msg = ClientMsg::Move {
                    actor: EntityId(actor),
                    dx: 1.0,
                    dy: 0.5,
                };
                send_env(&mut stream, cell, epoch, &mut seq, msg).await?;
                let reply: Envelope<ServerMsg> = recv(&mut stream).await?;
                println!("step {} <- {:?}", i + 1, reply);
            }
        }
        Cmd::Repl { actor } => {
            println!(
                "REPL started. Commands: ping <ts> | join <actor?> <x> <y> | move <actor?> <dx> <dy> | quit | help",
            );
            let mut rl = DefaultEditor::new()?;
            loop {
                match rl.readline("> ") {
                    Ok(line) => {
                        let _ = rl.add_history_entry(line.as_str());
                        if !handle_line(&line, &mut stream, actor, cell, epoch, &mut seq).await? {
                            break;
                        }
                    }
                    Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => break,
                    Err(e) => {
                        println!("error: {e:?}");
                        break;
                    }
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
                if !handle_line(&line, &mut stream, actor, cell, epoch, &mut seq).await? {
                    break;
                }
                line.clear();
            }
        }
    }

    Ok(())
}

async fn send_env(
    stream: &mut TcpStream,
    cell: CellId,
    epoch: u32,
    seq: &mut u64,
    msg: ClientMsg,
) -> Result<()> {
    let env = Envelope {
        cell,
        seq: *seq,
        epoch,
        payload: msg,
    };
    *seq = seq.wrapping_add(1);
    let frame = encode_frame(&env);
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

async fn handle_line(
    line: &str,
    stream: &mut TcpStream,
    actor: Option<u64>,
    cell: CellId,
    epoch: u32,
    seq: &mut u64,
) -> Result<bool> {
    let parts: Vec<_> = line.split_whitespace().collect();
    if parts.is_empty() {
        return Ok(true);
    }
    match parts[0] {
        "quit" | "exit" => return Ok(false),
        "help" => {
            println!(
                "commands: ping <ts> | join <actor?> <x> <y> | move <actor?> <dx> <dy> | sleep <ms> | quit"
            );
        }
        "ping" => {
            let ts: u64 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(1);
            send_env(stream, cell, epoch, seq, ClientMsg::Ping { ts }).await?;
            let r: Envelope<ServerMsg> = recv(stream).await?;
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
            send_env(
                stream,
                cell,
                epoch,
                seq,
                ClientMsg::Join {
                    actor: EntityId(aid),
                    pos: Position { x, y },
                },
            )
            .await?;
            let r: Envelope<ServerMsg> = recv(stream).await?;
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
            send_env(
                stream,
                cell,
                epoch,
                seq,
                ClientMsg::Move {
                    actor: EntityId(aid),
                    dx,
                    dy,
                },
            )
            .await?;
            let r: Envelope<ServerMsg> = recv(stream).await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parse_ping_defaults() {
        let cli = Cli::try_parse_from(["tessera-client", "ping"]).expect("parse");
        assert_eq!(cli.addr, "127.0.0.1:4000");
        assert_eq!(cli.world, 0);
        assert_eq!(cli.cx, 0);
        assert_eq!(cli.cy, 0);
        assert_eq!(cli.epoch, 0);
        match cli.cmd {
            Cmd::Ping { ts } => assert_eq!(ts, 1),
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parse_move_with_overrides() {
        let cli = Cli::try_parse_from([
            "tessera-client",
            "--addr",
            "10.0.0.1:4000",
            "--world",
            "2",
            "--cx=-1",
            "--cy",
            "3",
            "--epoch",
            "7",
            "move",
            "--actor",
            "5",
            "--dx",
            "1.25",
            "--dy=-0.5",
        ])
        .expect("parse");
        assert_eq!(cli.addr, "10.0.0.1:4000");
        assert_eq!(cli.world, 2);
        assert_eq!(cli.cx, -1);
        assert_eq!(cli.cy, 3);
        assert_eq!(cli.epoch, 7);
        match cli.cmd {
            Cmd::Move { actor, dx, dy } => {
                assert_eq!(actor, 5);
                assert_eq!(dx, 1.25);
                assert_eq!(dy, -0.5);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }
}
