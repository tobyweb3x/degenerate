use anyhow::{Ok, Result, ensure};
use polymarket_hft::client::polymarket::gamma::{self, helpers::deserialize_option_f64};
use qdrant_client::qdrant;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use uuid::Uuid;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PolymarketUsefulData {
    pub question: Option<String>,
    pub description: Option<String>,
    pub outcomes: Option<String>,
    #[serde(alias = "resolutionSource")]
    pub resolution_source: Option<String>,

    #[serde(alias = "conditionId")]
    pub condition_id: Option<String>,
    #[serde(alias = "questionID")]
    pub question_id: Option<String>,

    #[serde(alias = "endDate")]
    pub end_date: Option<String>,
    #[serde(alias = "startDate")]
    pub start_date: Option<String>,
    #[serde(alias = "closedTime")]
    pub closed_time: Option<String>,
    #[serde(alias = "endDateIso")]
    pub end_date_iso: Option<String>,

    #[serde(alias = "enableOrderBook")]
    pub enable_order_book: Option<bool>,
    #[serde(alias = "clobTokenIds")]
    pub clob_token_ids: Option<String>,
    #[serde(alias = "acceptingOrders")]
    pub accepting_orders: Option<bool>,
    pub ready: Option<bool>,
    pub active: Option<bool>,
    pub closed: Option<bool>,

    #[serde(alias = "negRiskOther")]
    pub neg_risk_other: Option<bool>,
    #[serde(
        default,
        deserialize_with = "deserialize_option_f64",
        alias = "orderPriceMinTickSize"
    )]
    pub order_price_min_tick_size: Option<f64>,
    #[serde(
        default,
        deserialize_with = "deserialize_option_f64",
        alias = "orderMinSize"
    )]
    pub order_min_size: Option<f64>,

    pub market_category: String,
    pub market_subcategory: String,
    pub platform: String,
}

impl From<gamma::Market> for PolymarketUsefulData {
    fn from(value: gamma::Market) -> Self {
        Self {
            question: value.question,
            description: value.description,
            outcomes: value.outcomes,
            resolution_source: value.resolution_source,

            condition_id: value.condition_id,
            question_id: value.question_id,

            end_date: value.end_date,
            start_date: value.start_date,
            closed_time: value.closed_time,
            end_date_iso: value.end_date_iso,

            enable_order_book: value.enable_order_book,
            clob_token_ids: value.clob_token_ids,
            accepting_orders: value.accepting_orders,
            ready: value.ready,
            active: value.active,
            closed: value.closed,

            neg_risk_other: value.neg_risk_other,
            order_price_min_tick_size: value.order_price_min_tick_size,
            order_min_size: value.order_min_size,
            market_category: String::new(),
            market_subcategory: String::new(),
            platform: String::new(),
        }
    }
}

impl PolymarketUsefulData {
    pub fn from_market(
        value: gamma::Market,
        market_category: String,
        market_subcategory: String,
    ) -> Self {
        let mut uselful_data: Self = value.into();
        uselful_data.platform = "Polymarket".to_string();
        uselful_data.market_category = market_category;
        uselful_data.market_subcategory = market_subcategory;
        uselful_data
    }

    pub fn from_markets(
        values: Vec<gamma::Market>,
        market_category: &str,
        market_subcategory: &str,
    ) -> Vec<Self> {
        let category = market_category.to_string();
        let subcategory = market_subcategory.to_string();
        let platform = "Polymarket".to_string();

        values
            .into_iter()
            .map(|value| {
                let mut data: Self = value.into();
                data.platform = platform.clone();
                data.market_category = category.clone();
                data.market_subcategory = subcategory.clone();
                data
            })
            .collect()
    }
}

#[derive(Deserialize, Serialize)]
pub struct QdrantPointData {
    id: String,
    question_vector: String,
    payload: QdrantPayload,
}

impl QdrantPointData {
    pub fn new(payload: QdrantPayload) -> Result<Self> {
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

    pub fn new_many(payloads: impl IntoIterator<Item = QdrantPayload>) -> Result<Vec<Self>> {
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

    pub fn get_payload(&self) -> QdrantPayload {
        self.payload.clone()
    }

    fn validate(payload: &QdrantPayload) -> Result<()> {
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
            !payload.description.trim().is_empty(),
            "description cannot be empty"
        );
        ensure!(
            !payload.condition_id.trim().is_empty(),
            "condition_id cannot be empty"
        );
        ensure!(
            !payload.market_category.trim().is_empty(),
            "market_category cannot be empty"
        );

        Ok(())
    }
}

pub trait QdrantMarketConverter<T> {
    fn from_market(
        value: T,
        category: impl Into<String>,
        subcategory: impl Into<String>,
    ) -> QdrantPayload;

    fn from_markets(
        values: Vec<T>,
        category: impl Into<String>,
        subcategory: impl Into<String>,
    ) -> Vec<QdrantPayload>;
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct QdrantPayload {
    pub uuid: String,
    pub question: String,
    pub outcome: String,
    pub clob_token_ids: String,
    pub description: String,
    pub condition_id: String,
    pub market_category: String,
    #[serde(default)]
    pub market_subcategory: String,
    pub platform: String,
    #[serde(default)]
    pub end_date: String,
}

impl From<PolymarketUsefulData> for QdrantPayload {
    fn from(value: PolymarketUsefulData) -> Self {
        Self {
            uuid: Uuid::new_v4().to_string(),
            question: value.question.unwrap_or_default(),
            outcome: value.outcomes.unwrap_or_default(),
            clob_token_ids: value.clob_token_ids.unwrap_or_default(),
            description: value.description.unwrap_or_default(),
            condition_id: value.condition_id.unwrap_or_default(),
            market_category: value.market_category,
            market_subcategory: value.market_subcategory,
            platform: value.platform,
            end_date: value.end_date_iso.or(value.end_date).unwrap_or_default(),
        }
    }
}

impl From<gamma::Market> for QdrantPayload {
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
            description: value.description.unwrap_or_default(),
            condition_id: value.condition_id.unwrap_or_default(),
            market_category: String::new(),
            market_subcategory: String::new(),
            platform: "polymarket".to_string(),
            end_date: value.end_date_iso.or(value.end_date).unwrap_or_default(),
        }
    }
}

impl From<kalshi_rs::markets::models::Market> for QdrantPayload {
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
            condition_id: value.ticker,
            description: {
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

impl TryFrom<HashMap<String, qdrant::Value>> for QdrantPayload {
    type Error = anyhow::Error;

    fn try_from(value: HashMap<String, qdrant::Value>) -> Result<Self, Self::Error> {
        let json = serde_json::to_value(value)?;
        let payload = serde_json::from_value(json)?;
        Ok(payload)
    }
}

impl QdrantMarketConverter<gamma::Market> for QdrantPayload {
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

impl QdrantMarketConverter<kalshi_rs::markets::models::Market> for QdrantPayload {
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

// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
// pub enum Platform {
//     Polymarket,
//     Kalshi,
//     Opinions,
//     Probable,
// }

// impl Platform {
//     pub const ALL: [Platform; 4] = [
//         Platform::Polymarket,
//         Platform::Kalshi,
//         Platform::Opinions,
//         Platform::Probable,
//     ];

//     pub fn names(self) -> &'static str {
//         match self {
//             Self::Polymarket => "polymarket",
//             Self::Kalshi => "kalshi",
//             Self::Opinions => "opinions",
//             Self::Probable => "probable",
//         }
//     }

//     pub fn others(self) -> Vec<Platform> {
//         Self::ALL.iter().copied().filter(|p| *p != self).collect()
//     }

//     pub fn other_platform_names(self) -> Vec<String> {
//         self.others()
//             .into_iter()
//             .map(|p| p.names().to_string())
//             .collect()
//     }
// }

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

#[derive(Debug)]
pub enum Todos {
    Similarity(SimilarityHit),
}

#[derive(Debug)]
pub struct SimilarityHit {
    pub take: QdrantPayload,
    pub gives: Vec<Give>,
}

impl SimilarityHit {
    pub fn try_from_results(take: QdrantPayload, gives: Vec<qdrant::ScoredPoint>) -> Result<Self> {
        let gives = gives
            .into_iter()
            .map(Give::try_from) // Uses the single impl above!
            .collect::<Result<Vec<Give>, anyhow::Error>>()?;

        Ok(Self { take, gives })
    }
}

impl fmt::Display for SimilarityHit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "==============start================")?;
        writeln!(f, "ANCHOR MARKET")?;
        writeln!(f, "Question : {}", self.take.question)?;
        writeln!(f, "Outcomes : {}", self.take.outcome)?;
        writeln!(f, "Platform : {}", self.take.platform)?;
        writeln!(f, "Market category : {}", self.take.market_category)?;
        writeln!(f, "Market subcategory : {}", self.take.market_subcategory)?;
        writeln!(f, "Rules : {}", self.take.description)?;
        writeln!(f, "ticker : {}", self.take.condition_id)?;
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

#[derive(Debug)]
pub struct Give {
    pub scored: f32,
    pub payload: QdrantPayload,
}

impl fmt::Display for Give {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "----------------------------------------")?;
        writeln!(f, "Question : {}", self.payload.question)?;
        writeln!(f, "Outcomes : {}", self.payload.outcome)?;
        writeln!(f, "Similarity : {:.2}%", self.scored * 100.0)?;
        writeln!(f, "Platform : {}", self.payload.platform)?;
        writeln!(f, "Market category : {}", self.payload.market_category)?;
        writeln!(
            f,
            "Market subcategory : {}",
            self.payload.market_subcategory
        )?;
        writeln!(f, "Rules : {}", self.payload.description)?;
        writeln!(f, "ticker : {}", self.payload.condition_id)?;

        fmt::Result::Ok(())
    }
}

impl TryFrom<qdrant::ScoredPoint> for Give {
    type Error = anyhow::Error;

    fn try_from(value: qdrant::ScoredPoint) -> Result<Self, Self::Error> {
        let payload = QdrantPayload::try_from(value.payload)?;
        Ok(Give {
            scored: value.score,
            payload,
        })
    }
}
