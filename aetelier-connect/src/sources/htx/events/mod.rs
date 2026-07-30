//! HTX WebSocket event types produced by
//! [`HtxDecoder`](crate::sources::htx::decoder::HtxDecoder).

use crate::sources::htx::responses::*;

#[derive(Debug, Clone)]
pub enum HtxWssEvent {
    MbpUpdate(HtxMbpUpdate),
    MbpSnapshot(HtxMbpSnapshot),
    Trade(HtxTradeFrame),
}
