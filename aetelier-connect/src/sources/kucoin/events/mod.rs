//! KuCoin WSS event types.
//!
//! Data `message` frames from the level2 (book) and match (trade) topics
//! decode into these variants; lifecycle frames are consumed by the decoder.

use crate::sources::kucoin::responses::{KucoinL2Data, KucoinMatchData};

#[derive(Debug, Clone)]
pub enum KucoinWssEvent {
    Level2(KucoinL2Data),
    Match(KucoinMatchData),
}
