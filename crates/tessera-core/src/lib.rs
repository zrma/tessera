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
    #[serde(default)]
    pub depth: u8,
    #[serde(default)]
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
    Error {
        code: String,
        message: String,
    },
    Snapshot {
        cell: CellId,
        actors: Vec<ActorState>,
    },
    Delta {
        cell: CellId,
        moved: Vec<ActorState>,
    },
    Despawn {
        cell: CellId,
        actors: Vec<EntityId>,
    },
}

// ---------- Envelope ----------
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Envelope<T> {
    pub cell: CellId,
    pub seq: u64,
    pub epoch: u32,
    pub payload: T,
}

// ---------- Length-prefixed framing (JSON payload, u32 BE length) ----------

/// 최대 프레임 길이(페이로드 기준). 네트워크 입력(신뢰할 수 없는 데이터)을 다룰 때
/// 메모리 DoS를 피하기 위해 상한을 둔다.
pub const MAX_FRAME_LEN: usize = 1_000_000;

pub fn encode_frame<T: Serialize>(value: &T) -> Bytes {
    let payload = serde_json::to_vec(value).expect("serialize frame");
    let mut buf = BytesMut::with_capacity(4 + payload.len());
    buf.put_u32(payload.len() as u32);
    buf.extend_from_slice(&payload);
    buf.freeze()
}

pub fn try_decode_frame<T: for<'de> Deserialize<'de>>(
    buf: &mut BytesMut,
) -> Result<Option<T>, serde_json::Error> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if len > MAX_FRAME_LEN {
        return Err(<serde_json::Error as serde::de::Error>::custom(format!(
            "frame length {} exceeds max {}",
            len, MAX_FRAME_LEN
        )));
    }
    let Some(total_len) = len.checked_add(4) else {
        return Err(<serde_json::Error as serde::de::Error>::custom(
            "frame length overflow",
        ));
    };
    if buf.len() < total_len {
        return Ok(None);
    }
    let payload = &buf[4..total_len];
    let decoded = serde_json::from_slice::<T>(payload)?;
    // Advance past length + payload only after successful decode.
    buf.advance(total_len);
    Ok(Some(decoded))
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
        let decoded: ClientMsg = try_decode_frame(&mut buf).expect("decode").expect("frame");
        assert_eq!(msg, decoded);
        // buffer should be drained
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn frame_roundtrip_envelope() {
        let env = Envelope {
            cell: CellId::grid(0, 1, 2),
            seq: 7,
            epoch: 0,
            payload: ServerMsg::Pong { ts: 123 },
        };
        let b = encode_frame(&env);
        let mut buf = BytesMut::from(&b[..]);
        let decoded: Envelope<ServerMsg> =
            try_decode_frame(&mut buf).expect("decode").expect("frame");
        assert_eq!(env, decoded);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn cellid_defaults_depth_and_sub() {
        let json = r#"{"world":1,"cx":-2,"cy":3}"#;
        let cell: CellId = serde_json::from_str(json).expect("decode cell");
        assert_eq!(cell.world, 1);
        assert_eq!(cell.cx, -2);
        assert_eq!(cell.cy, 3);
        assert_eq!(cell.depth, 0);
        assert_eq!(cell.sub, 0);
    }

    #[test]
    fn incomplete_frame_returns_none() {
        // Buffer too short to contain length
        let mut short_buf = BytesMut::from(&[0u8, 1, 2][..]);
        let result: Option<ClientMsg> = try_decode_frame(&mut short_buf).expect("decode");
        assert!(result.is_none());
        assert_eq!(short_buf.len(), 3);

        // Buffer declares a length larger than available payload
        let mut incomplete = BytesMut::from(&[0, 0, 0, 5, 1, 2, 3][..]);
        let result: Option<ClientMsg> = try_decode_frame(&mut incomplete).expect("decode");
        assert!(result.is_none());
        // Buffer should remain unchanged
        assert_eq!(incomplete.len(), 7);
    }

    #[test]
    fn decode_multiple_frames() {
        let msg1 = ClientMsg::Ping { ts: 1 };
        let msg2 = ClientMsg::Ping { ts: 2 };
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&encode_frame(&msg1));
        buf.extend_from_slice(&encode_frame(&msg2));

        let decoded1: ClientMsg = try_decode_frame(&mut buf)
            .expect("decode first")
            .expect("frame");
        assert_eq!(decoded1, msg1);
        let decoded2: ClientMsg = try_decode_frame(&mut buf)
            .expect("decode second")
            .expect("frame");
        assert_eq!(decoded2, msg2);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn invalid_json_does_not_consume_frame() {
        let payload = b"{not-json}";
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(payload);

        let result: Result<Option<ClientMsg>, _> = try_decode_frame(&mut buf);
        assert!(result.is_err());
        assert_eq!(buf.len(), 4 + payload.len());
    }

    #[test]
    fn frame_prefix_matches_payload_len() {
        let msg = ClientMsg::Ping { ts: 99 };
        let frame = encode_frame(&msg);
        let len = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
        assert_eq!(len, frame.len() - 4);
    }

    #[test]
    fn max_frame_length_returns_error_without_consuming() {
        let mut buf = BytesMut::from(&[0xFF, 0xFF, 0xFF, 0xFF][..]);
        let result: Result<Option<ClientMsg>, _> = try_decode_frame(&mut buf);
        assert!(result.is_err());
        assert_eq!(buf.len(), 4);
    }

    #[test]
    fn max_frame_length_allows_incomplete_frame() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&(MAX_FRAME_LEN as u32).to_be_bytes());
        let result: Option<ClientMsg> = try_decode_frame(&mut buf).expect("decode");
        assert!(result.is_none());
        assert_eq!(buf.len(), 4);
    }
}
