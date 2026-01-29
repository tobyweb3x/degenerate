use anyhow::{Ok, Result, ensure};

use polymarket_hft::client::polymarket::gamma::{self, helpers::deserialize_option_f64};
use serde::{Deserialize, Serialize};
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
        question_vector.push_str(&payload.question);
        let cleaned_outcome = payload.outcome.replace('\\', "");

        if !payload.question.ends_with('?') && !payload.question.ends_with('.') {
            println!("foud a question without ? or .");
            question_vector.push_str(". ");
            question_vector.push_str(&cleaned_outcome);
            question_vector.push_str(" (");
            question_vector.push_str(&payload.market_category);

            if !payload.market_subcategory.is_empty() {
                question_vector.push(':');
                question_vector.push_str(&payload.market_subcategory);
            }

            question_vector.push(')');
            return Ok(Self {
                id: payload.uuid.clone(),
                question_vector,
                payload,
            });
        }

        question_vector.push(' ');
        question_vector.push_str(&cleaned_outcome);
        question_vector.push_str(" (");
        question_vector.push_str(&payload.market_category);

        if !payload.market_subcategory.is_empty() {
            question_vector.push(':');
            question_vector.push_str(&payload.market_subcategory);
        }
        question_vector.push(')');

        println!("{question_vector}");
        Ok(Self {
            id: payload.uuid.clone(),
            question_vector,
            payload,
        })
    }

    pub fn new_many(payloads: impl IntoIterator<Item = QdrantPayload>) -> Result<Vec<Self>> {
        payloads
            .into_iter()
            .map(|payload| {
                let point = Self::new(payload)?;
                Ok(point)
            })
            .collect()
    }

    pub fn get_id(&self) -> &str {
        self.id.as_str()
    }

    pub fn get_question_vector(&self) -> &str {
        self.question_vector.as_str()
    }

    pub fn get_payload(&self) -> &QdrantPayload {
        &self.payload
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
            !payload.clob_token_ids.trim().is_empty(),
            "clobTokenIds cannot be empty"
        );
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
        ensure!(
            !payload.platform.trim().is_empty(),
            "platform cannot be empty"
        );
        Ok(())
    }
}

#[derive(Deserialize, Serialize, Clone)]
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
        QdrantPayload {
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
        QdrantPayload {
            uuid: Uuid::new_v4().to_string(),
            question: value.question.unwrap_or_default(),
            outcome: value.outcomes.unwrap_or_default(),
            clob_token_ids: value.clob_token_ids.unwrap_or_default(),
            description: value.description.unwrap_or_default(),
            condition_id: value.condition_id.unwrap_or_default(),
            market_category: String::new(),
            market_subcategory: String::new(),
            platform: String::new(),
            end_date: value.end_date_iso.or(value.end_date).unwrap_or_default(),
        }
    }
}

impl QdrantPayload {
    pub fn from_market(
        value: gamma::Market,
        market_category: String,
        market_subcategory: String,
    ) -> Self {
        let mut v: Self = value.into();
        v.platform = "Polymarket".to_string();
        v.market_category = market_category;
        v.market_subcategory = market_subcategory;
        v
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Platform {
    Polymarket,
    Kalshi,
    Opinions,
    Probable,
}

impl Platform {
    pub const ALL: [Platform; 4] = [
        Platform::Polymarket,
        Platform::Kalshi,
        Platform::Opinions,
        Platform::Probable,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Polymarket => "polymarket",
            Self::Kalshi => "kalshi",
            Self::Opinions => "opinions",
            Self::Probable => "probable",
        }
    }

    pub fn others(self) -> Vec<Platform> {
        Self::ALL.iter().copied().filter(|p| *p != self).collect()
    }

    pub fn other_platform_names(self) -> Vec<&'static str> {
        self.others().into_iter().map(|p| p.as_str()).collect()
    }
}
