//! Core types and simple length-prefixed framing for Tessera V0.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

// ---------- Basic Types ----------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntityId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Tick(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Position {
    pub x: f32,
    pub y: f32,
}

impl Position {
    pub fn new(x: f32, y: f32) -> Self {
        Self { x, y }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CellId {
    pub world: u32,
    pub cx: i32,
    pub cy: i32,
    pub depth: u8,
    pub sub: u8,
}

impl CellId {
    pub fn root(world: u32) -> Self {
        Self {
            world,
            cx: 0,
            cy: 0,
            depth: 0,
            sub: 0,
        }
    }

    pub fn grid(world: u32, cx: i32, cy: i32) -> Self {
        Self {
            world,
            cx,
            cy,
            depth: 0,
            sub: 0,
        }
    }
}

// ---------- Messages (V0) ----------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ActorState {
    pub id: EntityId,
    pub pos: Position,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMsg {
    Ping { ts: u64 },
    Join { actor: EntityId, pos: Position },
    Move { actor: EntityId, dx: f32, dy: f32 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    Pong {
        ts: u64,
    },
    Snapshot {
        cell: CellId,
        actors: Vec<ActorState>,
    },
    Delta {
        cell: CellId,
        moved: Vec<ActorState>,
    },
}

// ---------- Length-prefixed framing (JSON payload, u32 BE length) ----------

pub fn encode_frame<T: Serialize>(value: &T) -> Bytes {
    let payload = serde_json::to_vec(value).expect("serialize frame");
    let mut buf = BytesMut::with_capacity(4 + payload.len());
    buf.put_u32(payload.len() as u32);
    buf.extend_from_slice(&payload);
    buf.freeze()
}

pub fn try_decode_frame<T: for<'de> Deserialize<'de>>(buf: &mut BytesMut) -> Option<T> {
    if buf.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if buf.len() < 4 + len {
        return None;
    }
    // Advance past length
    buf.advance(4);
    let payload = buf.split_to(len);
    serde_json::from_slice::<T>(&payload).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cellid_helpers() {
        let c0 = CellId::root(1);
        assert_eq!(c0.world, 1);
        assert_eq!(c0.depth, 0);
        assert_eq!(c0.sub, 0);

        let c1 = CellId::grid(2, -3, 5);
        assert_eq!(c1.world, 2);
        assert_eq!(c1.cx, -3);
        assert_eq!(c1.cy, 5);
    }

    #[test]
    fn frame_roundtrip_client() {
        let msg = ClientMsg::Move {
            actor: EntityId(42),
            dx: 1.0,
            dy: -0.5,
        };
        let b = encode_frame(&msg);
        let mut buf = BytesMut::from(&b[..]);
        let decoded: ClientMsg = try_decode_frame(&mut buf).expect("decode");
        assert_eq!(msg, decoded);
        // buffer should be drained
        assert_eq!(buf.len(), 0);
    }
}
