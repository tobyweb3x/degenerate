use anyhow::{Context, Result, ensure};
use chrono::{DateTime, NaiveDate};
use polymarket_hft::client::polymarket::gamma;
use qdrant_client::{Payload, qdrant};

use once_cell::sync::Lazy;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromStr;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::json;
use std::{collections::HashMap, fmt};
use strum::IntoEnumIterator;
use strum_macros::EnumIter;
use uuid::Uuid;

pub mod protos {
    tonic::include_proto!("opon_ifa");
}

pub mod constants {
    pub const PLATFORM_POLYMARKET: &str = "POLYMARKET";
    pub const PLATFORM_KALSHI: &str = "KALSHI";
}

impl fmt::Display for protos::Platform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str_name())
    }
}

impl From<protos::Platform> for i64 {
    fn from(p: protos::Platform) -> Self {
        p as i32 as i64
    }
}

#[derive(Deserialize, Serialize)]
pub struct QdrantPointData {
    id: String,
    question_vector: String,
    payload: protos::QdrantPayload,
}

impl QdrantPointData {
    pub fn new(payload: protos::QdrantPayload) -> Result<Self> {
        let market_info = payload
            .market_info
            .as_ref()
            .context("market_info cannot be None")?;

        Self::validate(&market_info).context("QdrantPointData validation constraint failed")?;

        let mut question_vector = String::with_capacity(
            market_info.question.len()
                + market_info.outcome.len()
                + market_info.market_category.len()
                + 32,
        );

        question_vector.push_str("Question: ");
        question_vector.push_str(&market_info.question);

        question_vector.push(' ');
        question_vector.push_str("Possible outcomes: ");
        question_vector.push_str(&market_info.outcome);
        // question_vector.push_str(" Category: (");
        // question_vector.push_str(&market_info.market_category);

        // if !market_info.market_subcategory.is_empty() {
        //     question_vector.push(':');
        //     question_vector.push_str(&market_info.market_subcategory);
        // }
        // question_vector.push(')');

        // tracing::info!("question vector --> {question_vector}:{}", market_info.platform);
        Ok(Self {
            id: market_info.uuid.clone(),
            question_vector,
            payload,
        })
    }

    pub fn new_many(
        payloads: impl IntoIterator<Item = protos::QdrantPayload>,
    ) -> Result<Vec<Self>> {
        Ok(payloads
            .into_iter()
            .filter_map(|payload| match Self::new(payload) {
                Ok(point) => Some(point),
                Err(e) => {
                    tracing::error!("{e}");
                    None
                }
            })
            .collect())
    }

    pub fn get_id(&self) -> &str {
        self.id.as_str()
    }

    pub fn get_question_vector(&self) -> &str {
        self.question_vector.as_str()
    }

    pub fn get_payload(&self) -> protos::QdrantPayload {
        self.payload.clone()
    }

    fn validate(market_info: &protos::MarketInfo) -> Result<()> {
        ensure!(
            Uuid::try_parse(&market_info.uuid).is_ok(),
            "uuid string not valid uuid"
        );
        ensure!(
            !market_info.market_id.trim().is_empty(),
            "market_id cannot be empty"
        );
        ensure!(
            !market_info.question.trim().is_empty(),
            "question cannot be empty"
        );
        ensure!(
            !market_info.outcome.trim().is_empty(),
            "outcome cannot be empty"
        );
        ensure!(
            !market_info.rules.trim().is_empty(),
            "rules cannot be empty"
        );
        ensure!(
            !market_info.market_category.trim().is_empty(),
            "market_category cannot be empty"
        );
        ensure!(
            !market_info.market_subcategory.trim().is_empty(),
            "market_subcategory cannot be empty"
        );
        ensure!(
            protos::Platform::try_from(market_info.platform).is_ok(),
            "invalid platform"
        );
        ensure!(market_info.close_time_ms != 0, "close_time_ms cannot be 0");
        Ok(())
    }
}

impl TryFrom<protos::QdrantPayload> for QdrantPointData {
    type Error = anyhow::Error;

    fn try_from(value: protos::QdrantPayload) -> std::result::Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<QdrantPointData> for qdrant::PointStruct {
    type Error = anyhow::Error;

    fn try_from(value: QdrantPointData) -> Result<Self, Self::Error> {
        let payload = Payload::try_from(json!(value.get_payload()))
            .context("payload conversion to qdrant PointStruct failed")?;

        Ok(Self {
            payload: payload.into(),
            id: Some(qdrant::PointId::from(value.get_id())),
            ..Default::default()
        })
    }
}

fn parse_ts_to_millis(s: &str) -> i64 {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return dt.timestamp_millis();
    }

    // Fallback: YYYY-MM-DD
    if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d")
        && let Some(dt) = date.and_hms_opt(0, 0, 0)
    {
        return dt.and_utc().timestamp_millis();
    }

    // If everything fails
    0
}

pub fn generate_uuid_v5(seed: String) -> String {
    let uuid = Uuid::new_v5(&Uuid::NAMESPACE_OID, seed.as_bytes());
    uuid.to_string()
}

impl From<gamma::Market> for protos::MarketInfo {
    fn from(value: gamma::Market) -> Self {
        Self {
            uuid: generate_uuid_v5(format!(
                "{}:{}",
                protos::Platform::Polymarket.as_str_name(),
                value.slug.as_deref().unwrap_or_default()
            )),
            question: {
                let q = value.question.unwrap_or_default();
                if q.ends_with(['?', '.']) {
                    q
                } else {
                    format!("{q}.")
                }
            },
            outcome: value.outcomes.unwrap_or_default().replace('\\', ""),
            rules: value.description.unwrap_or_default(),
            market_id: value.slug.unwrap_or_default(),
            market_category: String::new(),
            market_subcategory: String::new(),
            platform: protos::Platform::Polymarket.into(),
            close_time_ms: parse_ts_to_millis(
                &value.end_date_iso.or(value.end_date).unwrap_or_default(),
            ),
        }
    }
}

impl From<kalshi_rs::markets::models::Market> for protos::MarketInfo {
    fn from(value: kalshi_rs::markets::models::Market) -> Self {
        Self {
            uuid: generate_uuid_v5(format!(
                "{}:{}",
                protos::Platform::Kalshi.as_str_name(),
                value.ticker.as_str(),
            )),
            question: {
                let q = if value.subtitle.trim().is_empty() {
                    value.title.trim().to_string()
                } else {
                    format!("{} {}", value.title.trim(), value.subtitle.trim())
                };

                if q.ends_with(['?', '.']) {
                    q
                } else {
                    format!("{q}.")
                }
            },
            outcome: {
                let mut outcome = String::new();
                outcome.push_str("[Yes(");
                outcome.push_str(value.yes_sub_title.as_str().trim());
                outcome.push_str("), No(Not ");
                outcome.push_str(value.no_sub_title.as_str().trim());
                outcome.push_str(")]");
                outcome
            },
            market_id: value.ticker,
            rules: {
                if value.rules_secondary.is_empty() {
                    value.rules_primary.clone()
                } else if value.rules_primary.ends_with('.') {
                    format!("{} {}", value.rules_primary, value.rules_secondary)
                } else {
                    format!("{}. {}", value.rules_primary, value.rules_secondary)
                }
            },
            market_category: String::new(),
            market_subcategory: String::new(),
            platform: protos::Platform::Kalshi.into(),
            close_time_ms: {
                let ts = if !value.close_time.is_empty() {
                    &value.close_time
                } else if let Some(ref exp) = value.expiration_time {
                    exp
                } else {
                    ""
                };

                parse_ts_to_millis(ts)
            },
        }
    }
}

impl From<gamma::Market> for protos::QdrantPayload {
    fn from(value: gamma::Market) -> Self {
        Self {
            market_info: Some(value.into()),
            qdrant_inserted_at: chrono::Utc::now().timestamp_millis(),
        }
    }
}

impl From<kalshi_rs::markets::models::Market> for protos::QdrantPayload {
    fn from(value: kalshi_rs::markets::models::Market) -> Self {
        Self {
            market_info: Some(value.into()),
            qdrant_inserted_at: chrono::Utc::now().timestamp_millis(),
        }
    }
}

impl TryFrom<HashMap<String, qdrant::Value>> for protos::QdrantPayload {
    type Error = anyhow::Error;

    fn try_from(value: HashMap<String, qdrant::Value>) -> Result<Self, Self::Error> {
        let json = serde_json::to_value(value)?;
        let payload = serde_json::from_value(json)?;
        Ok(payload)
    }
}

pub trait QdrantMarketConverter<T> {
    fn from_market(
        value: T,
        category: impl Into<String>,
        subcategory: impl Into<String>,
    ) -> protos::QdrantPayload;

    fn from_markets(
        values: Vec<T>,
        category: impl Into<String>,
        subcategory: impl Into<String>,
    ) -> Vec<protos::QdrantPayload>;
}

impl QdrantMarketConverter<gamma::Market> for protos::QdrantPayload {
    fn from_market(
        value: gamma::Market,
        market_category: impl Into<String>,
        market_subcategory: impl Into<String>,
    ) -> Self {
        let mut v: Self = value.into();
        let info = v.market_info.get_or_insert_with(Default::default);
        info.market_category = market_category.into();
        info.market_subcategory = market_subcategory.into();

        v
    }

    fn from_markets(
        values: Vec<gamma::Market>,
        market_category: impl Into<String>,
        market_subcategory: impl Into<String>,
    ) -> Vec<Self> {
        let market_category = market_category.into();
        let market_subcategory = market_subcategory.into();

        values
            .into_iter()
            .map(|value| {
                let mut v: Self = value.into();
                let info = v.market_info.get_or_insert_with(Default::default);
                info.market_category = market_category.clone();
                info.market_subcategory = market_subcategory.clone();

                v
            })
            .collect()
    }
}

impl QdrantMarketConverter<kalshi_rs::markets::models::Market> for protos::QdrantPayload {
    fn from_market(
        value: kalshi_rs::markets::models::Market,
        market_category: impl Into<String>,
        market_subcategory: impl Into<String>,
    ) -> Self {
        let mut v: Self = value.into();
        let info = v.market_info.get_or_insert_with(Default::default);
        info.market_category = market_category.into();
        info.market_subcategory = market_subcategory.into();

        v
    }

    fn from_markets(
        values: Vec<kalshi_rs::markets::models::Market>,
        market_category: impl Into<String>,
        market_subcategory: impl Into<String>,
    ) -> Vec<Self> {
        let market_category = market_category.into();
        let market_subcategory = market_subcategory.into();

        values
            .into_iter()
            .map(|value| {
                let mut v: Self = value.into();
                let info = v.market_info.get_or_insert_with(Default::default);
                info.market_category = market_category.clone();
                info.market_subcategory = market_subcategory.clone();

                v
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumIter)]
pub enum MarketTag {
    Soccer,
    Nba,
    Nfl,
    Politics,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MarketTagInfo {
    pub market_category: &'static str,
    pub market_subcategory: &'static str,
}

impl MarketTag {
    pub fn identifiers(&self, platform: protos::Platform) -> &'static [&'static str] {
        match (self, platform) {
            (Self::Nba, protos::Platform::Polymarket) => &[
                "745", "100240", "100871", "102037", "101506", "102288", "100108", "100254",
                "101849", "100283", "102769",
            ],
            (Self::Nba, protos::Platform::Kalshi) => &["Basketball"],

            (Self::Soccer, protos::Platform::Polymarket) => &["306", "100350", "100834", "100300"],
            (Self::Soccer, protos::Platform::Kalshi) => &["Soccer"],

            (Self::Nfl, protos::Platform::Polymarket) => &[
                "450", "101674", "100959", "1453", "100833", "102575", "102590", "636", "102160",
                "101594", "1186", "102934", "10", "101673",
            ],
            (Self::Nfl, protos::Platform::Kalshi) => &["Football"],

            (Self::Politics, protos::Platform::Polymarket) => &[
                "2", "596", "176", "789", "902", "100199", "144", "101206", "102788", "1597",
                "102006", "1515", "264", "126", "103026", "102514", "102477", "230", "393",
                "100434", "1396", "778", "828", "915", "859", "1346", "101773", "101426", "80",
                "100783", "102810", "766", "514", "718", "1459", "1405", "1400", "247", "1337",
                "282", "396", "538", "791", "100343", "100381", "100387", "100388",
            ],
            (Self::Politics, protos::Platform::Kalshi) => {
                &["Politics", "Elections", "Companies", "Mentions"]
            }
        }
    }

    pub fn info(&self) -> MarketTagInfo {
        match self {
            Self::Soccer => MarketTagInfo {
                market_category: "sport",
                market_subcategory: "Socer",
            },
            Self::Nba => MarketTagInfo {
                market_category: "sport",
                market_subcategory: "NBA",
            },
            Self::Nfl => MarketTagInfo {
                market_category: "sport",
                market_subcategory: "NFL",
            },
            Self::Politics => MarketTagInfo {
                market_category: "politics",
                market_subcategory: "Politics",
            },
        }
    }
}

impl fmt::Display for MarketTagInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}_{}", self.market_category, self.market_subcategory,)
    }
}

pub static POLYMARKET_TAG_LOOKUP: Lazy<HashMap<&'static str, MarketTag>> = Lazy::new(|| {
    let mut map = HashMap::new();

    for market in MarketTag::iter() {
        for &tag in market.identifiers(protos::Platform::Polymarket) {
            map.insert(tag, market);
        }
    }

    map
});

pub static KALSHI_TAG_LOOKUP: Lazy<HashMap<&'static str, MarketTag>> = Lazy::new(|| {
    let mut map = HashMap::new();

    for market in MarketTag::iter() {
        for &tag in market.identifiers(protos::Platform::Kalshi) {
            map.insert(tag, market);
        }
    }

    map
});

impl TryFrom<qdrant::ScoredPoint> for protos::MatchCandidate {
    type Error = anyhow::Error;

    fn try_from(value: qdrant::ScoredPoint) -> Result<Self, Self::Error> {
        let payload = protos::QdrantPayload::try_from(value.payload)?;
        Ok(protos::MatchCandidate {
            scored: value.score,
            market_info: payload.market_info,
        })
    }
}

impl protos::SimilarityHit {
    pub fn try_from_results(
        anchor: protos::QdrantPayload,
        matches: Vec<qdrant::ScoredPoint>,
    ) -> Result<Self> {
        let matches = matches
            .into_iter()
            .map(protos::MatchCandidate::try_from)
            .collect::<Result<Vec<protos::MatchCandidate>, anyhow::Error>>()?;

        Ok(Self {
            anchor: anchor.market_info,
            matches,
        })
    }
}

impl fmt::Display for protos::SimilarityHit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(anchor) = &self.anchor else {
            writeln!(f, "ANCHOR MARKET: <missing>")?;
            return Ok(());
        };

        writeln!(f, "==============start================")?;
        writeln!(f, "ANCHOR MARKET")?;
        writeln!(f, "Question : {}", anchor.question)?;
        writeln!(f, "Outcomes : {}", anchor.outcome)?;
        writeln!(f, "Platform : {}", anchor.platform)?;
        writeln!(f, "Market category : {}", anchor.market_category)?;
        writeln!(f, "Market subcategory : {}", anchor.market_subcategory)?;
        writeln!(f, "Rules : {}", anchor.rules)?;
        writeln!(f, "ticker : {}", anchor.market_id)?;

        writeln!(f)?;

        writeln!(f, "CANDIDATE MATCHES: {}", self.matches.len())?;

        for (i, match_candidate) in self.matches.iter().enumerate() {
            writeln!(f, "\nMatch #{}", i + 1)?;
            writeln!(f, "{match_candidate}")?;
        }
        writeln!(f, "==============end================")?;

        Ok(())
    }
}

impl fmt::Display for protos::MatchCandidate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(r#match) = &self.market_info else {
            return Ok(());
        };

        writeln!(f, "----------------------------------------")?;
        writeln!(f, "Question : {}", r#match.question)?;
        writeln!(f, "Outcomes : {}", r#match.outcome)?;
        writeln!(f, "Similarity : {:.2}%", &self.scored * 100.0)?;
        writeln!(f, "Platform : {}", r#match.platform)?;
        writeln!(f, "Market category : {}", r#match.market_category)?;
        writeln!(f, "Market subcategory : {}", r#match.market_subcategory)?;
        writeln!(f, "Rules : {}", r#match.rules)?;
        writeln!(f, "ticker : {}", r#match.market_id)?;

        Ok(())
    }
}

pub fn de_str_to_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    s.parse::<f64>().map_err(serde::de::Error::custom)
}

pub fn de_str_to_i64<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    s.parse::<i64>().map_err(serde::de::Error::custom)
}

pub fn parse_f32(s: &str) -> anyhow::Result<f32> {
    Ok(s.parse::<f32>().context("error parsing string to f32")?)
}

/// MarketKey should be the asset_id for polymarket and market_ticker for kalshi
pub fn make_market_key(asset_id: &str, platform: protos::Platform) -> MarketKey {
    format!("{}:{}", platform.as_str_name(), asset_id)
}

/// MarketKey should be the asset_id for polymarket and market_ticker for kalshi
pub type MarketKey = String;
pub type CorrelationId = String;

#[derive(Debug, Clone)]
pub struct ArbMinifiedInfo {
    pub market_key: MarketKey,
    pub platform: protos::Platform,
    pub market_id: String,
    pub token_id: String,
    pub leg: protos::Leg,
    pub close_time_ms: i64,
}

#[derive(Debug, Clone)]
pub struct ArbMinifiedInfoPair {
    pub anchor: ArbMinifiedInfo,
    pub r#match: ArbMinifiedInfo,
    pub _scored: f32,
}

pub struct ArbWatch {
    pub correlation_id: CorrelationId,
    pub arb: ArbMinifiedInfoPair,
}

#[derive(Debug, Clone, Default)]
pub struct TopOfBook {
    pub best_bid: f32,
    pub bid_size: f32,
    pub best_ask: f32,
    pub ask_size: f32,
    pub spread: f32,
    pub tob_timestamp_ms: i64,
    pub sid: Option<i64>,
}

impl TryFrom<protos::ArbEssentials> for ArbMinifiedInfo {
    type Error = anyhow::Error;

    fn try_from(essentials: protos::ArbEssentials) -> Result<Self, Self::Error> {
        let discovery = essentials
            .discovery
            .context("Missing 'discovery' in ArbEssentials")?;

        let market_info = discovery
            .market_info
            .context("Missing 'market_info' in Discovery")?;

        if market_info.market_id.is_empty() {
            anyhow::bail!("market_id cannot be empty")
        }

        if market_info.close_time_ms == 0 {
            anyhow::bail!("close_time_ms cannot be 0")
        }

        if essentials.token_id.is_empty() {
            anyhow::bail!("token_id cannot be empty")
        }

        let platform = protos::Platform::try_from(market_info.platform)
            .context(format!("Invalid platform ID: {}", market_info.platform))?;

        let leg = protos::Leg::try_from(discovery.leg)
            .context(format!("Invalid leg ID: {}", discovery.leg))?;

        Ok(Self {
            market_key: make_market_key(essentials.token_id.as_str(), platform),
            market_id: market_info.market_id,
            token_id: essentials.token_id,
            platform,
            leg,
            close_time_ms: market_info.close_time_ms,
        })
    }
}

impl TryFrom<protos::Arb> for ArbMinifiedInfoPair {
    type Error = anyhow::Error;

    fn try_from(arb: protos::Arb) -> Result<Self, Self::Error> {
        let anchor_essentials = arb.anchor.context("Missing 'anchor' in Arb")?;
        let match_essentials = arb.r#match.context("Missing 'match' in Arb")?;

        Ok(Self {
            anchor: ArbMinifiedInfo::try_from(anchor_essentials)?,
            r#match: ArbMinifiedInfo::try_from(match_essentials)?,
            _scored: arb.scored,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionRequest {
    pub correlation_id: CorrelationId,
    pub anchor: ArbMinifiedInfo,
    pub r#match: ArbMinifiedInfo,
    pub anchor_price: f32,
    pub match_price: f32,
}

pub fn str_to_decimal(value: &str) -> anyhow::Result<Decimal> {
    let dec = Decimal::from_str(value)
        .map_err(|e| anyhow::anyhow!("Failed to parse '{}' into Decimal: {}", value, e))?;

    Ok(dec)
}

pub fn opt_str_to_decimal_strict(value: &Option<String>) -> anyhow::Result<Decimal> {
    value
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Expected value but got None"))
        .and_then(str_to_decimal)
}

