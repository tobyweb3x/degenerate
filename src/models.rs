use crate::constants;
use anyhow::{Context, Ok, Result, ensure};
use polymarket_hft::client::polymarket::gamma;
use qdrant_client::{Payload, qdrant};

use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::fmt;
use uuid::Uuid;

pub mod protos {
    tonic::include_proto!("similarityhit");
}

#[derive(Deserialize, Serialize)]
pub struct QdrantPointData {
    id: String,
    question_vector: String,
    payload: protos::QdrantPayload,
}

impl QdrantPointData {
    pub fn new(payload: protos::QdrantPayload) -> Result<Self> {
        Self::validate(&payload)?;

        let mut question_vector = String::with_capacity(
            payload.question.len() + payload.outcome.len() + payload.market_category.len() + 32,
        );

        question_vector.push_str("Question: ");
        question_vector.push_str(&payload.question);

        question_vector.push(' ');
        question_vector.push_str("Outcomes: ");
        question_vector.push_str(&payload.outcome);
        question_vector.push_str(" Category: (");
        question_vector.push_str(&payload.market_category);

        if !payload.market_subcategory.is_empty() {
            question_vector.push(':');
            question_vector.push_str(&payload.market_subcategory);
        }
        question_vector.push(')');

        // tracing::info!("question vector --> {question_vector}:{}", payload.platform);
        Ok(Self {
            id: payload.uuid.clone(),
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
                std::result::Result::Ok(point) => Some(point),
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

    fn validate(payload: &protos::QdrantPayload) -> Result<()> {
        ensure!(
            Uuid::try_parse(&payload.uuid).is_ok(),
            "uuid string not valid uuid"
        );
        ensure!(
            !payload.question.trim().is_empty(),
            "question cannot be empty"
        );
        ensure!(
            !payload.outcome.trim().is_empty(),
            "outcome cannot be empty"
        );

        ensure!(
            !payload.platform.trim().is_empty(),
            "platform cannot be empty"
        );

        if payload.platform == constants::PLATFORM_POLYMARKET {
            ensure!(
                !payload.clob_token_ids.trim().is_empty(),
                "clobTokenIds cannot be empty"
            );
        }

        ensure!(
            !payload.rules.trim().is_empty(),
            "description cannot be empty"
        );
        ensure!(
            !payload.market_id.trim().is_empty(),
            "condition_id cannot be empty"
        );
        ensure!(
            !payload.market_category.trim().is_empty(),
            "market_category cannot be empty"
        );

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

pub fn generate_point_id(platform: &str, market_id: &str) -> String {
    let seed = format!("{}:{}", platform, market_id);
    let uuid = Uuid::new_v5(&Uuid::NAMESPACE_OID, seed.as_bytes());
    uuid.to_string()
}

impl From<gamma::Market> for protos::QdrantPayload {
    fn from(value: gamma::Market) -> Self {
        Self {
            uuid: generate_point_id(
                constants::PLATFORM_POLYMARKET,
                value.condition_id.as_deref().unwrap_or_default(),
            ),
            question: {
                let q = value.question.unwrap_or_default();
                if q.ends_with(|c| c == '?' || c == '.') {
                    q
                } else {
                    format!("{q}.")
                }
            },
            outcome: value.outcomes.unwrap_or_default().replace('\\', ""),
            clob_token_ids: value.clob_token_ids.unwrap_or_default(),
            rules: value.description.unwrap_or_default(),
            market_id: value.condition_id.unwrap_or_default(),
            market_category: String::new(),
            market_subcategory: String::new(),
            platform: constants::PLATFORM_POLYMARKET.to_string(),
            end_date: value.end_date_iso.or(value.end_date).unwrap_or_default(),
            inserted_at: chrono::Utc::now().timestamp_millis(),
        }
    }
}

impl From<kalshi_rs::markets::models::Market> for protos::QdrantPayload {
    fn from(value: kalshi_rs::markets::models::Market) -> Self {
        Self {
            uuid: generate_point_id(constants::PLATFORM_KALSHI, &value.ticker),
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
            clob_token_ids: String::new(),
            market_category: String::new(),
            market_subcategory: String::new(),
            platform: constants::PLATFORM_KALSHI.to_string(),
            end_date: value.close_time,
            inserted_at: chrono::Utc::now().timestamp_millis(),
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
        v.market_category = market_category.into();
        v.market_subcategory = market_subcategory.into();
        v
    }

    fn from_markets(
        values: Vec<gamma::Market>,
        market_category: impl Into<String>,
        market_subcategory: impl Into<String>,
    ) -> Vec<Self> {
        let category = market_category.into();
        let subcategory = market_subcategory.into();

        values
            .into_iter()
            .map(|value| {
                let mut data: Self = value.into();
                data.market_category = category.clone();
                data.market_subcategory = subcategory.clone();
                data
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
        v.market_category = market_category.into();
        v.market_subcategory = market_subcategory.into();
        v
    }

    fn from_markets(
        values: Vec<kalshi_rs::markets::models::Market>,
        market_category: impl Into<String>,
        market_subcategory: impl Into<String>,
    ) -> Vec<Self> {
        let category = market_category.into();
        let subcategory = market_subcategory.into();

        values
            .into_iter()
            .map(|value| {
                let mut data: Self = value.into();
                data.market_category = category.clone();
                data.market_subcategory = subcategory.clone();
                data
            })
            .collect()
    }
}

pub enum MarketTag {
    EPL,
    NBA,
    NFL,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MarketTagInfo {
    pub polymarket_identifier: &'static str,
    pub kalshi_identifier: &'static str,
    pub market_category: &'static str,
    pub market_subcategory: &'static str,
}

impl std::fmt::Display for MarketTagInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_{}", self.market_category, self.market_subcategory,)
    }
}

impl MarketTag {
    pub fn info(&self) -> MarketTagInfo {
        match self {
            Self::EPL => MarketTagInfo {
                polymarket_identifier: "306",
                kalshi_identifier: "Soccer",
                market_category: "sport",
                market_subcategory: "EPL",
            },
            Self::NBA => MarketTagInfo {
                polymarket_identifier: "745",
                kalshi_identifier: "Basketball",
                market_category: "sport",
                market_subcategory: "NBA",
            },
            Self::NFL => MarketTagInfo {
                polymarket_identifier: "450",
                kalshi_identifier: "Football",
                market_category: "sport",
                market_subcategory: "NFL",
            },
        }
    }
}

impl TryFrom<qdrant::ScoredPoint> for protos::Matches {
    type Error = anyhow::Error;

    fn try_from(value: qdrant::ScoredPoint) -> Result<Self, Self::Error> {
        let payload = protos::QdrantPayload::try_from(value.payload)?;
        Ok(protos::Matches {
            scored: value.score,
            r#match: Some(payload),
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
            .map(protos::Matches::try_from)
            .collect::<Result<Vec<protos::Matches>, anyhow::Error>>()?;

        Ok(Self {
            anchor: Some(anchor),
            matches,
        })
    }
}

impl fmt::Display for protos::SimilarityHit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(anchor) = &self.anchor else {
            writeln!(f, "ANCHOR MARKET: <missing>")?;
            return fmt::Result::Ok(());
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

        for (i, r#match) in self.matches.iter().enumerate() {
            writeln!(f, "\nMatch #{}", i + 1)?;
            writeln!(f, "{match}")?;
        }
        writeln!(f, "==============end================")?;

        fmt::Result::Ok(())
    }
}

impl fmt::Display for protos::Matches {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(r#match) = &self.r#match else {
            return fmt::Result::Ok(());
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

        fmt::Result::Ok(())
    }
}
