use anyhow::{Ok, Result, ensure};
use polymarket_hft::client::polymarket::gamma;
use qdrant_client::qdrant;
use serde::{Deserialize, Serialize};
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

        // println!("question vector --> {question_vector}:{}", payload.platform);
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
            Uuid::parse_str(payload.uuid.as_str()).is_ok(),
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

        if payload.platform == "polymarket" {
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

impl From<gamma::Market> for protos::QdrantPayload {
    fn from(value: gamma::Market) -> Self {
        Self {
            uuid: Uuid::new_v4().to_string(),
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
            platform: "polymarket".to_string(),
            end_date: value.end_date_iso.or(value.end_date).unwrap_or_default(),
        }
    }
}

impl From<kalshi_rs::markets::models::Market> for protos::QdrantPayload {
    fn from(value: kalshi_rs::markets::models::Market) -> Self {
        Self {
            uuid: Uuid::new_v4().to_string(),
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
                outcome.push_str(") : No(Not ");
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
            platform: "kalshi".to_string(),
            end_date: value.close_time,
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
    pub category: &'static str,
    pub subcategory: &'static str,
}

impl std::fmt::Display for MarketTagInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}_{}_{}",
            self.category, self.subcategory, self.polymarket_identifier
        )
    }
}

impl MarketTag {
    pub fn info(&self) -> MarketTagInfo {
        match self {
            Self::EPL => MarketTagInfo {
                polymarket_identifier: "306",
                kalshi_identifier: "Soccer",
                category: "sport",
                subcategory: "EPL",
            },
            Self::NBA => MarketTagInfo {
                polymarket_identifier: "745",
                kalshi_identifier: "Basketball",
                category: "sport",
                subcategory: "NBA",
            },
            Self::NFL => MarketTagInfo {
                polymarket_identifier: "450",
                kalshi_identifier: "Football",
                category: "sport",
                subcategory: "NFL",
            },
        }
    }
}

impl protos::SimilarityHit {
    pub fn try_from_results(
        take: protos::QdrantPayload,
        gives: Vec<qdrant::ScoredPoint>,
    ) -> Result<Self> {
        let gives = gives
            .into_iter()
            .map(protos::Give::try_from) // Uses the single impl above!
            .collect::<Result<Vec<protos::Give>, anyhow::Error>>()?;

        Ok(Self {
            take: Some(take),
            gives,
        })
    }
}

impl fmt::Display for protos::SimilarityHit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(take) = &self.take else {
            writeln!(f, "ANCHOR MARKET: <missing>")?;
            return fmt::Result::Ok(());
        };

        writeln!(f, "==============start================")?;
        writeln!(f, "ANCHOR MARKET")?;
        writeln!(f, "Question : {}", take.question)?;
        writeln!(f, "Outcomes : {}", take.outcome)?;
        writeln!(f, "Platform : {}", take.platform)?;
        writeln!(f, "Market category : {}", take.market_category)?;
        writeln!(f, "Market subcategory : {}", take.market_subcategory)?;
        writeln!(f, "Rules : {}", take.rules)?;
        writeln!(f, "ticker : {}", take.market_id)?;

        writeln!(f)?;

        writeln!(f, "CANDIDATE MATCHES: {}", self.gives.len())?;

        for (i, give) in self.gives.iter().enumerate() {
            writeln!(f, "\nMatch #{}", i + 1)?;
            writeln!(f, "{give}")?;
        }
        writeln!(f, "==============end================")?;

        fmt::Result::Ok(())
    }
}

impl fmt::Display for protos::Give {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(payload) = &self.payload else {
            return fmt::Result::Ok(());
        };

        writeln!(f, "----------------------------------------")?;
        writeln!(f, "Question : {}", payload.question)?;
        writeln!(f, "Outcomes : {}", payload.outcome)?;
        writeln!(f, "Similarity : {:.2}%", &self.scored * 100.0)?;
        writeln!(f, "Platform : {}", payload.platform)?;
        writeln!(f, "Market category : {}", payload.market_category)?;
        writeln!(f, "Market subcategory : {}", payload.market_subcategory)?;
        writeln!(f, "Rules : {}", payload.rules)?;
        writeln!(f, "ticker : {}", payload.market_id)?;

        fmt::Result::Ok(())
    }
}

impl TryFrom<qdrant::ScoredPoint> for protos::Give {
    type Error = anyhow::Error;

    fn try_from(value: qdrant::ScoredPoint) -> Result<Self, Self::Error> {
        let payload = protos::QdrantPayload::try_from(value.payload)?;
        Ok(protos::Give {
            scored: value.score,
            payload: Some(payload),
        })
    }
}
