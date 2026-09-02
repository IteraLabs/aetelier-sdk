use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::errors::BuildError;
use crate::trading_pair::TradingPair;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenInterest {
    pub open_interest_ts_us: u64,
    #[serde(default)]
    pub local_oi_ts_us: u64,
    #[serde(default)]
    pub recv_seq: u64,
    #[serde(default)]
    pub conn_epoch_us: u64,
    pub pair: TradingPair,
    pub open_interest: Decimal,
    #[serde(default)]
    pub open_interest_value: Option<Decimal>,
    #[serde(default)]
    pub mark_px: Option<Decimal>,
    pub exchange: String,
}

impl OpenInterest {
    pub fn builder() -> OpenInterestBuilder {
        OpenInterestBuilder::new()
    }

    pub fn effective_ts_us(&self) -> u64 {
        if self.open_interest_ts_us > 0 {
            self.open_interest_ts_us
        } else {
            self.local_oi_ts_us
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct OpenInterestBuilder {
    open_interest_ts_us: Option<u64>,
    local_oi_ts_us: Option<u64>,
    recv_seq: Option<u64>,
    conn_epoch_us: Option<u64>,
    pair: Option<TradingPair>,
    open_interest: Option<Decimal>,
    open_interest_value: Option<Decimal>,
    mark_px: Option<Decimal>,
    exchange: Option<String>,
}

impl OpenInterestBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open_interest_ts_us(mut self, open_interest_ts_us: u64) -> Self {
        self.open_interest_ts_us = Some(open_interest_ts_us);
        self
    }

    pub fn local_oi_ts_us(mut self, local_oi_ts_us: u64) -> Self {
        self.local_oi_ts_us = Some(local_oi_ts_us);
        self
    }

    pub fn recv_seq(mut self, recv_seq: u64) -> Self {
        self.recv_seq = Some(recv_seq);
        self
    }

    pub fn conn_epoch_us(mut self, conn_epoch_us: u64) -> Self {
        self.conn_epoch_us = Some(conn_epoch_us);
        self
    }

    pub fn pair(mut self, pair: TradingPair) -> Self {
        self.pair = Some(pair);
        self
    }

    pub fn open_interest(mut self, oi: Decimal) -> Self {
        self.open_interest = Some(oi);
        self
    }

    pub fn open_interest_value(mut self, value: Decimal) -> Self {
        self.open_interest_value = Some(value);
        self
    }

    pub fn mark_px(mut self, mark_px: Decimal) -> Self {
        self.mark_px = Some(mark_px);
        self
    }

    pub fn exchange(mut self, exchange: String) -> Self {
        self.exchange = Some(exchange);
        self
    }

    pub fn build(self) -> Result<OpenInterest, BuildError> {
        let open_interest_ts_us = self.open_interest_ts_us.unwrap_or(0);
        let local_oi_ts_us = self.local_oi_ts_us.unwrap_or(0);
        if open_interest_ts_us == 0 && local_oi_ts_us == 0 {
            return Err(BuildError::MissingField("ts"));
        }
        let pair = self.pair.ok_or(BuildError::MissingField("pair"))?;
        let open_interest = self
            .open_interest
            .ok_or(BuildError::MissingField("open_interest"))?;
        let exchange = self.exchange.ok_or(BuildError::MissingField("exchange"))?;

        Ok(OpenInterest {
            open_interest_ts_us,
            local_oi_ts_us,
            recv_seq: self.recv_seq.unwrap_or(0),
            conn_epoch_us: self.conn_epoch_us.unwrap_or(0),
            pair,
            open_interest,
            open_interest_value: self.open_interest_value,
            mark_px: self.mark_px,
            exchange,
        })
    }
}
