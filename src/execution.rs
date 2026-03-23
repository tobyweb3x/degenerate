use crate::models;
use crate::picker;

impl picker::Picker {}

#[derive(Debug, Clone)]
pub struct ExecutionRequest {
    pub correlation_id: String,
    pub anchor: models::ArbMinifiedInfo,
    pub match_leg: models::ArbMinifiedInfo,
    pub anchor_price: f32,
    pub match_price: f32,
}
