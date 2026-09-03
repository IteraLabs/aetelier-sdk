use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::errors::BuildError;
use crate::trading_pair::TradingPair;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingRate {
    pub funding_rate_ts_us: u64,
    #[serde(default)]
    pub local_funding_ts_us: u64,
    #[serde(default)]
    pub recv_seq: u64,
    #[serde(default)]
    pub conn_epoch_us: u64,
    pub pair: TradingPair,
    pub funding_rate: Decimal,
    #[serde(default)]
    pub premium: Option<Decimal>,
    pub interval_hours: u32,
    pub next_funding_ts_us: u64,
    pub exchange: String,
}

impl FundingRate {
    pub fn builder() -> FundingRateBuilder {
        FundingRateBuilder::new()
    }

    pub fn effective_ts_us(&self) -> u64 {
        if self.funding_rate_ts_us > 0 {
            self.funding_rate_ts_us
        } else {
            self.local_funding_ts_us
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct FundingRateBuilder {
    funding_rate_ts_us: Option<u64>,
    local_funding_ts_us: Option<u64>,
    recv_seq: Option<u64>,
    conn_epoch_us: Option<u64>,
    pair: Option<TradingPair>,
    funding_rate: Option<Decimal>,
    premium: Option<Decimal>,
    interval_hours: Option<u32>,
    next_funding_ts_us: Option<u64>,
    exchange: Option<String>,
}

impl FundingRateBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn funding_rate_ts_us(mut self, funding_rate_ts_us: u64) -> Self {
        self.funding_rate_ts_us = Some(funding_rate_ts_us);
        self
    }

    pub fn local_funding_ts_us(mut self, local_funding_ts_us: u64) -> Self {
        self.local_funding_ts_us = Some(local_funding_ts_us);
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

    pub fn funding_rate(mut self, rate: Decimal) -> Self {
        self.funding_rate = Some(rate);
        self
    }

    pub fn premium(mut self, premium: Decimal) -> Self {
        self.premium = Some(premium);
        self
    }

    pub fn interval_hours(mut self, interval_hours: u32) -> Self {
        self.interval_hours = Some(interval_hours);
        self
    }

    pub fn next_funding_ts_us(mut self, ts: u64) -> Self {
        self.next_funding_ts_us = Some(ts);
        self
    }

    pub fn exchange(mut self, exchange: String) -> Self {
        self.exchange = Some(exchange);
        self
    }

    pub fn build(self) -> Result<FundingRate, BuildError> {
        let funding_rate_ts_us = self.funding_rate_ts_us.unwrap_or(0);
        let local_funding_ts_us = self.local_funding_ts_us.unwrap_or(0);
        if funding_rate_ts_us == 0 && local_funding_ts_us == 0 {
            return Err(BuildError::MissingField("ts"));
        }
        let pair = self.pair.ok_or(BuildError::MissingField("pair"))?;
        let funding_rate = self
            .funding_rate
            .ok_or(BuildError::MissingField("funding_rate"))?;
        let interval_hours = self
            .interval_hours
            .ok_or(BuildError::MissingField("interval_hours"))?;
        if interval_hours == 0 {
            return Err(BuildError::MissingField("interval_hours"));
        }
        let exchange = self.exchange.ok_or(BuildError::MissingField("exchange"))?;

        Ok(FundingRate {
            funding_rate_ts_us,
            local_funding_ts_us,
            recv_seq: self.recv_seq.unwrap_or(0),
            conn_epoch_us: self.conn_epoch_us.unwrap_or(0),
            pair,
            funding_rate,
            premium: self.premium,
            interval_hours,
            next_funding_ts_us: self.next_funding_ts_us.unwrap_or(0),
            exchange,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingSettlement {
    pub funding_time_us: u64,
    pub local_ts_us: u64,
    pub rtt_us: u64,
    pub pair: TradingPair,
    pub funding_rate: Decimal,
    #[serde(default)]
    pub premium: Option<Decimal>,
    pub exchange: String,
}

impl FundingSettlement {
    pub fn builder() -> FundingSettlementBuilder {
        FundingSettlementBuilder::new()
    }
}

#[derive(Debug, Clone, Default)]
pub struct FundingSettlementBuilder {
    funding_time_us: Option<u64>,
    local_ts_us: Option<u64>,
    rtt_us: Option<u64>,
    pair: Option<TradingPair>,
    funding_rate: Option<Decimal>,
    premium: Option<Decimal>,
    exchange: Option<String>,
}

impl FundingSettlementBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn funding_time_us(mut self, funding_time_us: u64) -> Self {
        self.funding_time_us = Some(funding_time_us);
        self
    }

    pub fn local_ts_us(mut self, local_ts_us: u64) -> Self {
        self.local_ts_us = Some(local_ts_us);
        self
    }

    pub fn rtt_us(mut self, rtt_us: u64) -> Self {
        self.rtt_us = Some(rtt_us);
        self
    }

    pub fn pair(mut self, pair: TradingPair) -> Self {
        self.pair = Some(pair);
        self
    }

    pub fn funding_rate(mut self, rate: Decimal) -> Self {
        self.funding_rate = Some(rate);
        self
    }

    pub fn premium(mut self, premium: Decimal) -> Self {
        self.premium = Some(premium);
        self
    }

    pub fn exchange(mut self, exchange: String) -> Self {
        self.exchange = Some(exchange);
        self
    }

    pub fn build(self) -> Result<FundingSettlement, BuildError> {
        let funding_time_us = self
            .funding_time_us
            .ok_or(BuildError::MissingField("funding_time_us"))?;
        if funding_time_us == 0 {
            return Err(BuildError::MissingField("funding_time_us"));
        }
        let pair = self.pair.ok_or(BuildError::MissingField("pair"))?;
        let funding_rate = self
            .funding_rate
            .ok_or(BuildError::MissingField("funding_rate"))?;
        let exchange = self.exchange.ok_or(BuildError::MissingField("exchange"))?;

        Ok(FundingSettlement {
            funding_time_us,
            local_ts_us: self.local_ts_us.unwrap_or(0),
            rtt_us: self.rtt_us.unwrap_or(0),
            pair,
            funding_rate,
            premium: self.premium,
            exchange,
        })
    }
}
