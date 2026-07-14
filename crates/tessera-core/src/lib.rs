//! Core types and simple length-prefixed framing for Tessera V0.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt;

pub const RUNTIME_LOG_FORMAT_ENV: &str = "TESSERA_LOG_FORMAT";
pub const REQUEST_LIFECYCLE_TARGET: &str = "tessera.request.lifecycle";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RuntimeLogFormat {
    #[default]
    Compact,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnsupportedRuntimeLogFormat;

impl RuntimeLogFormat {
    pub fn parse(raw: Option<&str>) -> Result<Self, UnsupportedRuntimeLogFormat> {
        match raw {
            None | Some("compact") => Ok(Self::Compact),
            Some("json") => Ok(Self::Json),
            Some(_) => Err(UnsupportedRuntimeLogFormat),
        }
    }
}

impl fmt::Display for UnsupportedRuntimeLogFormat {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("must be unset, compact, or json")
    }
}

impl std::error::Error for UnsupportedRuntimeLogFormat {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestOperation {
    Join,
    Move,
}

impl RequestOperation {
    pub fn from_client_message(message: &ClientMsg) -> Option<Self> {
        match message {
            ClientMsg::Join { .. } => Some(Self::Join),
            ClientMsg::Move { .. } => Some(Self::Move),
            ClientMsg::Ping { .. } => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Join => "join",
            Self::Move => "move",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestLifecycleEvent {
    GatewayRequestForwarded,
    WorkerRequestReceived,
    WorkerResponseSent,
    GatewayResponseForwarded,
}

impl RequestLifecycleEvent {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GatewayRequestForwarded => "gateway.request.forwarded",
            Self::WorkerRequestReceived => "worker.request.received",
            Self::WorkerResponseSent => "worker.response.sent",
            Self::GatewayResponseForwarded => "gateway.response.forwarded",
        }
    }

    pub const fn component(self) -> &'static str {
        match self {
            Self::GatewayRequestForwarded | Self::GatewayResponseForwarded => "gateway",
            Self::WorkerRequestReceived | Self::WorkerResponseSent => "worker",
        }
    }
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellChildFamilyKind {
    CanonicalLeaf,
    LegacyShallow,
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

    pub fn leaf(world: u32, cx: i32, cy: i32, depth: u8) -> Self {
        Self {
            world,
            cx,
            cy,
            depth,
            sub: 0,
        }
    }

    pub fn is_canonical_leaf(self) -> bool {
        self.sub == 0
    }

    pub fn canonical_child(self, quadrant: u8) -> Option<Self> {
        if !self.is_canonical_leaf() || quadrant > 3 {
            return None;
        }
        let depth = self.depth.checked_add(1)?;
        let qx = i32::from(quadrant & 1);
        let qy = i32::from((quadrant >> 1) & 1);
        Some(Self::leaf(
            self.world,
            self.cx.checked_mul(2)?.checked_add(qx)?,
            self.cy.checked_mul(2)?.checked_add(qy)?,
            depth,
        ))
    }

    pub fn canonical_parent(self) -> Option<Self> {
        if self.depth == 0 || !self.is_canonical_leaf() {
            return None;
        }
        Some(Self::leaf(
            self.world,
            self.cx.div_euclid(2),
            self.cy.div_euclid(2),
            self.depth - 1,
        ))
    }

    pub fn canonical_quadrant(self) -> Option<u8> {
        if self.depth == 0 || !self.is_canonical_leaf() {
            return None;
        }
        let qx = self.cx.rem_euclid(2) as u8;
        let qy = self.cy.rem_euclid(2) as u8;
        Some(qx + (qy << 1))
    }

    pub fn canonical_siblings(self) -> Option<[Self; 4]> {
        let parent = self.canonical_parent()?;
        Some([
            parent.canonical_child(0)?,
            parent.canonical_child(1)?,
            parent.canonical_child(2)?,
            parent.canonical_child(3)?,
        ])
    }

    pub fn canonical_children(self) -> Option<[Self; 4]> {
        Some([
            self.canonical_child(0)?,
            self.canonical_child(1)?,
            self.canonical_child(2)?,
            self.canonical_child(3)?,
        ])
    }

    pub fn legacy_shallow_child(self, sub: u8) -> Option<Self> {
        if self.depth != 0 || self.sub != 0 || sub > 3 {
            return None;
        }
        Some(Self {
            world: self.world,
            cx: self.cx,
            cy: self.cy,
            depth: 1,
            sub,
        })
    }

    pub fn legacy_shallow_children(self) -> Option<[Self; 4]> {
        Some([
            self.legacy_shallow_child(0)?,
            self.legacy_shallow_child(1)?,
            self.legacy_shallow_child(2)?,
            self.legacy_shallow_child(3)?,
        ])
    }

    pub fn child_family_kind(self, children: &[Self]) -> Option<CellChildFamilyKind> {
        if self
            .legacy_shallow_children()
            .is_some_and(|expected| cell_set_eq(children, &expected))
        {
            return Some(CellChildFamilyKind::LegacyShallow);
        }
        if self
            .canonical_children()
            .is_some_and(|expected| cell_set_eq(children, &expected))
        {
            return Some(CellChildFamilyKind::CanonicalLeaf);
        }
        None
    }
}

fn cell_set_eq(actual: &[CellId], expected: &[CellId; 4]) -> bool {
    actual.len() == expected.len()
        && expected
            .iter()
            .all(|needle| actual.iter().filter(|actual| *actual == needle).count() == 1)
}

// ---------- Messages (V0) ----------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ActorState {
    pub id: EntityId,
    pub pos: Position,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HandoverReplayMove {
    pub actor: EntityId,
    pub dx: f32,
    pub dy: f32,
    pub epoch: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HandoverReplayOwner {
    pub actor: EntityId,
    pub owner_session: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SplitReplayTarget {
    pub cell: CellId,
    pub target_worker_id: String,
    pub target_addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MergeReplayTarget {
    pub parent: CellId,
    pub source_cell: CellId,
    pub target_worker_id: String,
    pub target_addr: String,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkerRelayMsg {
    Auth {
        token: String,
    },
    Subscribe {
        cells: Vec<CellId>,
    },
    Unsubscribe {
        cells: Vec<CellId>,
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
    HandoverReplay {
        operation_id: String,
        cell: CellId,
        actors: Vec<ActorState>,
        #[serde(default)]
        owners: Vec<HandoverReplayOwner>,
        moves: Vec<HandoverReplayMove>,
    },
    HandoverReplayAck {
        operation_id: String,
        cell: CellId,
        accepted: bool,
        reason: String,
    },
    SplitReplayPrepare {
        operation_id: String,
        cells: Vec<CellId>,
    },
    SplitReplayRequest {
        operation_id: String,
        parent: CellId,
        children: Vec<SplitReplayTarget>,
    },
    SplitReplayAbort {
        operation_id: String,
        cells: Vec<CellId>,
    },
    SplitReplayAck {
        operation_id: String,
        parent: CellId,
        accepted: bool,
        reason: String,
        children: Vec<CellId>,
    },
    MergeReplayPrepare {
        operation_id: String,
        parent: CellId,
    },
    MergeReplayRequest {
        operation_id: String,
        target: MergeReplayTarget,
    },
    MergeReplayAbort {
        operation_id: String,
        parent: CellId,
        source_cell: Option<CellId>,
    },
    MergeReplayAck {
        operation_id: String,
        parent: CellId,
        source_cell: Option<CellId>,
        accepted: bool,
        reason: String,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ClientEnvelope {
    pub cell: CellId,
    pub seq: u64,
    pub epoch: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<u64>,
    pub payload: ClientMsg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestTraceContext {
    pub operation: RequestOperation,
    pub session_id: u64,
    pub request_id: u64,
    pub cell: CellId,
}

impl RequestTraceContext {
    pub fn from_client_envelope(envelope: &ClientEnvelope) -> Option<Self> {
        Some(Self {
            operation: RequestOperation::from_client_message(&envelope.payload)?,
            session_id: envelope.session?,
            request_id: envelope.request_id?,
            cell: envelope.cell,
        })
    }
}

impl From<Envelope<ClientMsg>> for ClientEnvelope {
    fn from(env: Envelope<ClientMsg>) -> Self {
        Self {
            cell: env.cell,
            seq: env.seq,
            epoch: env.epoch,
            session: None,
            request_id: None,
            payload: env.payload,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ServerEnvelope {
    pub cell: CellId,
    pub seq: u64,
    pub epoch: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<u64>,
    pub payload: ServerMsg,
}

impl From<Envelope<ServerMsg>> for ServerEnvelope {
    fn from(env: Envelope<ServerMsg>) -> Self {
        Self {
            cell: env.cell,
            seq: env.seq,
            epoch: env.epoch,
            request_id: None,
            payload: env.payload,
        }
    }
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
    fn runtime_log_format_defaults_to_compact() {
        assert_eq!(RuntimeLogFormat::parse(None), Ok(RuntimeLogFormat::Compact));
        assert_eq!(RuntimeLogFormat::default(), RuntimeLogFormat::Compact);
    }

    #[test]
    fn runtime_log_format_accepts_only_canonical_values() {
        assert_eq!(
            RuntimeLogFormat::parse(Some("compact")),
            Ok(RuntimeLogFormat::Compact)
        );
        assert_eq!(
            RuntimeLogFormat::parse(Some("json")),
            Ok(RuntimeLogFormat::Json)
        );
    }

    #[test]
    fn runtime_log_format_rejects_unsupported_values() {
        for raw in ["", "JSON", "pretty", " json "] {
            assert_eq!(
                RuntimeLogFormat::parse(Some(raw)),
                Err(UnsupportedRuntimeLogFormat)
            );
        }
    }

    #[test]
    fn request_lifecycle_event_contract_is_stable_and_component_scoped() {
        let events = [
            RequestLifecycleEvent::GatewayRequestForwarded,
            RequestLifecycleEvent::WorkerRequestReceived,
            RequestLifecycleEvent::WorkerResponseSent,
            RequestLifecycleEvent::GatewayResponseForwarded,
        ];
        assert_eq!(
            events.map(RequestLifecycleEvent::as_str),
            [
                "gateway.request.forwarded",
                "worker.request.received",
                "worker.response.sent",
                "gateway.response.forwarded",
            ]
        );
        assert_eq!(
            events.map(RequestLifecycleEvent::component),
            ["gateway", "worker", "worker", "gateway"]
        );
    }

    #[test]
    fn request_trace_context_keeps_only_correlation_fields() {
        let cell = CellId::leaf(7, -2, 3, 4);
        let join = ClientEnvelope {
            cell,
            seq: 9,
            epoch: 11,
            session: Some(13),
            request_id: Some(17),
            payload: ClientMsg::Join {
                actor: EntityId(19),
                pos: Position::new(1.0, 2.0),
            },
        };
        assert_eq!(
            RequestTraceContext::from_client_envelope(&join),
            Some(RequestTraceContext {
                operation: RequestOperation::Join,
                session_id: 13,
                request_id: 17,
                cell,
            })
        );

        let mut move_request = join.clone();
        move_request.payload = ClientMsg::Move {
            actor: EntityId(23),
            dx: 3.0,
            dy: 4.0,
        };
        assert_eq!(
            RequestTraceContext::from_client_envelope(&move_request)
                .expect("move trace context")
                .operation,
            RequestOperation::Move
        );

        move_request.payload = ClientMsg::Ping { ts: 29 };
        assert_eq!(
            RequestTraceContext::from_client_envelope(&move_request),
            None
        );
        move_request.payload = ClientMsg::Move {
            actor: EntityId(23),
            dx: 3.0,
            dy: 4.0,
        };
        move_request.session = None;
        assert_eq!(
            RequestTraceContext::from_client_envelope(&move_request),
            None
        );
    }

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
    fn canonical_leaf_children_use_leaf_resolution_coordinates() {
        let parent = CellId::leaf(7, -2, 3, 2);
        assert_eq!(parent.canonical_child(0), Some(CellId::leaf(7, -4, 6, 3)));
        assert_eq!(parent.canonical_child(1), Some(CellId::leaf(7, -3, 6, 3)));
        assert_eq!(parent.canonical_child(2), Some(CellId::leaf(7, -4, 7, 3)));
        assert_eq!(parent.canonical_child(3), Some(CellId::leaf(7, -3, 7, 3)));
        assert_eq!(parent.canonical_child(4), None);
    }

    #[test]
    fn canonical_leaf_parent_and_quadrant_are_coordinate_derived() {
        let child = CellId::leaf(3, -3, 7, 3);
        assert_eq!(child.canonical_parent(), Some(CellId::leaf(3, -2, 3, 2)));
        assert_eq!(child.canonical_quadrant(), Some(3));
        assert_eq!(
            child.canonical_siblings(),
            Some([
                CellId::leaf(3, -4, 6, 3),
                CellId::leaf(3, -3, 6, 3),
                CellId::leaf(3, -4, 7, 3),
                CellId::leaf(3, -3, 7, 3),
            ])
        );
    }

    #[test]
    fn child_family_kind_classifies_legacy_shallow_family() {
        let parent = CellId::grid(0, 4, -2);
        let children = [
            parent.legacy_shallow_child(2).expect("legacy child"),
            parent.legacy_shallow_child(0).expect("legacy child"),
            parent.legacy_shallow_child(3).expect("legacy child"),
            parent.legacy_shallow_child(1).expect("legacy child"),
        ];

        assert_eq!(
            parent.child_family_kind(&children),
            Some(CellChildFamilyKind::LegacyShallow)
        );
    }

    #[test]
    fn child_family_kind_classifies_canonical_leaf_family() {
        let parent = CellId::leaf(0, -2, 3, 2);
        let children = [
            parent.canonical_child(3).expect("canonical child"),
            parent.canonical_child(1).expect("canonical child"),
            parent.canonical_child(0).expect("canonical child"),
            parent.canonical_child(2).expect("canonical child"),
        ];

        assert_eq!(
            parent.child_family_kind(&children),
            Some(CellChildFamilyKind::CanonicalLeaf)
        );
    }

    #[test]
    fn child_family_kind_rejects_mixed_or_duplicate_family() {
        let parent = CellId::grid(0, 0, 0);
        let mixed = [
            parent.legacy_shallow_child(0).expect("legacy child"),
            parent.legacy_shallow_child(1).expect("legacy child"),
            parent.canonical_child(2).expect("canonical child"),
            parent.canonical_child(3).expect("canonical child"),
        ];
        assert_eq!(parent.child_family_kind(&mixed), None);

        let duplicate = [
            parent.legacy_shallow_child(0).expect("legacy child"),
            parent.legacy_shallow_child(0).expect("legacy child"),
            parent.legacy_shallow_child(2).expect("legacy child"),
            parent.legacy_shallow_child(3).expect("legacy child"),
        ];
        assert_eq!(parent.child_family_kind(&duplicate), None);
        assert_eq!(parent.child_family_kind(&duplicate[..3]), None);
    }

    #[test]
    fn canonical_helpers_reject_legacy_sub_only_cells() {
        let parent = CellId::grid(0, 0, 0);
        let legacy = parent.legacy_shallow_child(2).expect("legacy child");
        assert_eq!(
            legacy,
            CellId {
                world: 0,
                cx: 0,
                cy: 0,
                depth: 1,
                sub: 2,
            }
        );
        assert_eq!(legacy.canonical_parent(), None);
        assert_eq!(legacy.canonical_child(0), None);
        assert_eq!(legacy.canonical_quadrant(), None);
        assert_eq!(parent.legacy_shallow_child(4), None);
        assert_eq!(legacy.legacy_shallow_child(0), None);
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
    fn client_envelope_accepts_missing_session() {
        let env = Envelope {
            cell: CellId::grid(0, 1, 2),
            seq: 7,
            epoch: 9,
            payload: ClientMsg::Ping { ts: 123 },
        };
        let b = encode_frame(&env);
        let mut buf = BytesMut::from(&b[..]);
        let decoded: ClientEnvelope = try_decode_frame(&mut buf).expect("decode").expect("frame");
        assert_eq!(decoded.cell, env.cell);
        assert_eq!(decoded.seq, env.seq);
        assert_eq!(decoded.epoch, env.epoch);
        assert_eq!(decoded.session, None);
        assert_eq!(decoded.request_id, None);
        assert_eq!(decoded.payload, env.payload);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn client_envelope_roundtrip_session() {
        let env = ClientEnvelope {
            cell: CellId::grid(0, 1, 2),
            seq: 7,
            epoch: 9,
            session: Some(42),
            request_id: Some(99),
            payload: ClientMsg::Ping { ts: 123 },
        };
        let b = encode_frame(&env);
        let mut buf = BytesMut::from(&b[..]);
        let decoded: ClientEnvelope = try_decode_frame(&mut buf).expect("decode").expect("frame");
        assert_eq!(decoded, env);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn server_envelope_accepts_missing_request_id() {
        let env = Envelope {
            cell: CellId::grid(0, 1, 2),
            seq: 7,
            epoch: 0,
            payload: ServerMsg::Pong { ts: 123 },
        };
        let b = encode_frame(&env);
        let mut buf = BytesMut::from(&b[..]);
        let decoded: ServerEnvelope = try_decode_frame(&mut buf).expect("decode").expect("frame");
        assert_eq!(decoded.cell, env.cell);
        assert_eq!(decoded.seq, env.seq);
        assert_eq!(decoded.epoch, env.epoch);
        assert_eq!(decoded.request_id, None);
        assert_eq!(decoded.payload, env.payload);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn server_envelope_roundtrip_request_id() {
        let env = ServerEnvelope {
            cell: CellId::grid(0, 1, 2),
            seq: 7,
            epoch: 0,
            request_id: Some(101),
            payload: ServerMsg::Pong { ts: 123 },
        };
        let b = encode_frame(&env);
        let mut buf = BytesMut::from(&b[..]);
        let decoded: ServerEnvelope = try_decode_frame(&mut buf).expect("decode").expect("frame");
        assert_eq!(env, decoded);
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
    fn frame_roundtrip_worker_relay() {
        let env = Envelope {
            cell: CellId::grid(0, 1, 0),
            seq: 3,
            epoch: 9,
            payload: WorkerRelayMsg::Subscribe {
                cells: vec![CellId::grid(0, 1, 0), CellId::grid(0, 2, 0)],
            },
        };
        let b = encode_frame(&env);
        let mut buf = BytesMut::from(&b[..]);
        let decoded: Envelope<WorkerRelayMsg> =
            try_decode_frame(&mut buf).expect("decode").expect("frame");
        assert_eq!(env, decoded);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn frame_roundtrip_handover_replay() {
        let cell = CellId::grid(0, 1, 0);
        let env = Envelope {
            cell,
            seq: 4,
            epoch: 12,
            payload: WorkerRelayMsg::HandoverReplay {
                operation_id: "handover-1".to_string(),
                cell,
                actors: vec![ActorState {
                    id: EntityId(7),
                    pos: Position::new(1.0, 2.0),
                }],
                owners: vec![HandoverReplayOwner {
                    actor: EntityId(7),
                    owner_session: 42,
                }],
                moves: vec![HandoverReplayMove {
                    actor: EntityId(7),
                    dx: 3.0,
                    dy: 4.0,
                    epoch: 13,
                }],
            },
        };
        let b = encode_frame(&env);
        let mut buf = BytesMut::from(&b[..]);
        let decoded: Envelope<WorkerRelayMsg> =
            try_decode_frame(&mut buf).expect("decode").expect("frame");
        assert_eq!(env, decoded);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn frame_roundtrip_split_replay_request_and_ack() {
        let parent = CellId::grid(0, 1, 0);
        let child = CellId {
            world: 0,
            cx: 1,
            cy: 0,
            depth: 1,
            sub: 2,
        };
        let env = Envelope {
            cell: parent,
            seq: 5,
            epoch: 13,
            payload: WorkerRelayMsg::SplitReplayRequest {
                operation_id: "split-1".to_string(),
                parent,
                children: vec![SplitReplayTarget {
                    cell: child,
                    target_worker_id: "worker-b".to_string(),
                    target_addr: "127.0.0.1:5002".to_string(),
                }],
            },
        };
        let b = encode_frame(&env);
        let mut buf = BytesMut::from(&b[..]);
        let decoded: Envelope<WorkerRelayMsg> =
            try_decode_frame(&mut buf).expect("decode").expect("frame");
        assert_eq!(env, decoded);
        assert_eq!(buf.len(), 0);

        let ack = Envelope {
            cell: parent,
            seq: 6,
            epoch: 13,
            payload: WorkerRelayMsg::SplitReplayAck {
                operation_id: "split-1".to_string(),
                parent,
                accepted: true,
                reason: "split replay applied".to_string(),
                children: vec![child],
            },
        };
        let b = encode_frame(&ack);
        let mut buf = BytesMut::from(&b[..]);
        let decoded: Envelope<WorkerRelayMsg> =
            try_decode_frame(&mut buf).expect("decode").expect("frame");
        assert_eq!(ack, decoded);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn frame_roundtrip_merge_replay_request_and_ack() {
        let parent = CellId::grid(0, 1, 0);
        let source_cell = CellId {
            world: 0,
            cx: 1,
            cy: 0,
            depth: 1,
            sub: 2,
        };
        let env = Envelope {
            cell: source_cell,
            seq: 5,
            epoch: 13,
            payload: WorkerRelayMsg::MergeReplayRequest {
                operation_id: "merge-1".to_string(),
                target: MergeReplayTarget {
                    parent,
                    source_cell,
                    target_worker_id: "worker-a".to_string(),
                    target_addr: "127.0.0.1:5001".to_string(),
                },
            },
        };
        let b = encode_frame(&env);
        let mut buf = BytesMut::from(&b[..]);
        let decoded: Envelope<WorkerRelayMsg> =
            try_decode_frame(&mut buf).expect("decode").expect("frame");
        assert_eq!(env, decoded);
        assert_eq!(buf.len(), 0);

        let ack = Envelope {
            cell: source_cell,
            seq: 6,
            epoch: 13,
            payload: WorkerRelayMsg::MergeReplayAck {
                operation_id: "merge-1".to_string(),
                parent,
                source_cell: Some(source_cell),
                accepted: true,
                reason: "merge replay applied".to_string(),
            },
        };
        let b = encode_frame(&ack);
        let mut buf = BytesMut::from(&b[..]);
        let decoded: Envelope<WorkerRelayMsg> =
            try_decode_frame(&mut buf).expect("decode").expect("frame");
        assert_eq!(ack, decoded);
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
