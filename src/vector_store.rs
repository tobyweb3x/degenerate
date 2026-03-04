use crate::models::{self, protos};
use anyhow::{Context, Ok, Result};
use candle_core::Device;
use chrono::{DateTime, Utc};
use qdrant_client::{
    Qdrant,
    qdrant::{
        Condition, CreateCollectionBuilder, CreateFieldIndexCollectionBuilder, DeletePointsBuilder,
        Direction, FieldType, Filter, HnswConfigDiffBuilder, OrderBy, PointId, PointStruct,
        PointsIdsList, Query, QueryBatchPointsBuilder, QueryBatchResponse, QueryPoints,
        QueryPointsBuilder, QueryResponse, ScrollPointsBuilder, UpdateCollectionBuilder,
        UpsertPointsBuilder, VectorInput, VectorParamsBuilder, r#match::MatchValue,
    },
};
use sentence_transformers_rs::sentence_transformer::{
    SentenceTransformer, SentenceTransformerBuilder, Which,
};
use sha2::Digest;
use sha2::Sha256;
use std::convert::TryFrom;
use std::sync::Arc;
use tokio::{sync::mpsc, task};

pub const COLLECTION_NAME: &str = "arb_hit";

// Payload Field Names
pub const FIELD_UUID: &str = "uuid";
pub const FIELD_MARKET_CATEGORY: &str = "market_category";
pub const FIELD_PLATFORM: &str = "platform";
pub const FIELD_MARKET_SUBCATEGORY: &str = "market_subcategory";
pub const FIELD_END_DATE: &str = "end_date";
pub const FIELD_INSERTED_AT: &str = "inserted_at";

pub const SIMILARITY_SCORE_THRESHOLD: f32 = 0.65;

#[derive(Clone)]
pub struct VectorStore {
    qdrant_client: Qdrant,
    collection_name: &'static str,
    semantic_model: Arc<SentenceTransformer>,
    tx: mpsc::Sender<protos::Ebo>,
}

impl VectorStore {
    pub async fn new_metal(
        url: &str,
        collection_name: &'static str,
        tx: mpsc::Sender<protos::Ebo>,
    ) -> Result<Self> {
        let device =
            candle_core::Device::new_metal(0).context("metal device initialization failed")?;

        let vs = Self::new(url, collection_name, device, tx).await?;
        tracing::info!("succesfully setup vector store");
        Ok(vs)
    }

    async fn new(
        url: &str,
        collection_name: &'static str,
        device: Device,
        tx: mpsc::Sender<protos::Ebo>,
    ) -> Result<Self> {
        let client = Qdrant::from_url(url)
            .skip_compatibility_check()
            .build()
            .context("error building qdrant config")?;

        if !client.collection_exists(collection_name).await? {
            client
                .create_collection(
                    CreateCollectionBuilder::new(collection_name).vectors_config(
                        VectorParamsBuilder::new(384, qdrant_client::qdrant::Distance::Cosine),
                    ),
                )
                .await
                .context("error creating new qdrant collection")?;
        }

        let model = SentenceTransformerBuilder::with_sentence_transformer(&Which::AllMiniLML12v2)
            .batch_size(2048)
            .with_device(&device)
            .build()
            .context("sentenceTransformer build failed")?;

        Ok(Self {
            qdrant_client: client,
            collection_name,
            semantic_model: Arc::new(model),
            tx,
        })
    }

    pub async fn disable_hnsw(&self) -> Result<()> {
        self.qdrant_client
            .update_collection(
                UpdateCollectionBuilder::new(self.collection_name)
                    .hnsw_config(HnswConfigDiffBuilder::default().m(0)),
            )
            .await
            .context("failed to disable HNSW indexing")?;

        Ok(())
    }

    pub async fn enable_hnsw(&self, edges_per_node: u64) -> Result<()> {
        let edges_per_node = if edges_per_node == 0 {
            32
        } else {
            edges_per_node
        };

        self.qdrant_client
            .update_collection(
                UpdateCollectionBuilder::new(self.collection_name)
                    .hnsw_config(HnswConfigDiffBuilder::default().m(edges_per_node)),
            )
            .await
            .context("failed to enable HNSW indexing")?;

        tracing::info!("payload-index enabled");
        Ok(())
    }

    async fn index_payload(&self, field_name: &str, field_type: FieldType) -> Result<()> {
        if field_name.is_empty() {
            return Ok(());
        }
        self.qdrant_client
            .create_field_index(
                CreateFieldIndexCollectionBuilder::new(
                    self.collection_name,
                    field_name,
                    field_type,
                )
                .wait(true),
            )
            .await?;

        Ok(())
    }

    pub async fn setup_qdrant_payload_index(&self) -> Result<()> {
        self.index_payload(FIELD_UUID, FieldType::Uuid)
            .await
            .context(format!("qdrant index error on {}", FIELD_UUID))?;
        self.index_payload(FIELD_MARKET_CATEGORY, FieldType::Keyword)
            .await
            .context(format!("qdrant index error on {}", FIELD_MARKET_CATEGORY))?;
        self.index_payload(FIELD_PLATFORM, FieldType::Keyword)
            .await
            .context(format!("qdrant index error on {}", FIELD_PLATFORM))?;
        self.index_payload(FIELD_MARKET_SUBCATEGORY, FieldType::Keyword)
            .await
            .context(format!("qdrant index error on {}", FIELD_MARKET_CATEGORY))?;
        self.index_payload(FIELD_END_DATE, FieldType::Datetime)
            .await
            .context(format!("qdrant index error on {}", FIELD_END_DATE))?;
        self.index_payload(FIELD_INSERTED_AT, FieldType::Integer) // Changed
            .await
            .context(format!("qdrant index error on {}", FIELD_INSERTED_AT))?;

        Ok(())
    }

    pub async fn semantic_similarity_search(
        &self,
        vector: impl Into<VectorInput>,
        score_threshold: f32,
        limit: u64,
        filter: Option<Filter>,
        with_paylod: bool,
    ) -> Result<QueryResponse> {
        let limit = if limit == 0 { 10 } else { limit };
        let score_threshold = if score_threshold == 0.0 {
            0.8
        } else {
            score_threshold
        };

        let mut request = QueryPointsBuilder::new(self.collection_name)
            .query(Query::new_nearest(vector))
            .score_threshold(score_threshold)
            .limit(limit)
            .with_payload(with_paylod);

        if let Some(f) = filter {
            request = request.filter(f);
        }

        let response = self.qdrant_client.query(request.build()).await?;

        Ok(response)
    }

    pub async fn semantic_similarity_search_batch(
        &self,
        searches: Vec<QueryPoints>,
    ) -> Result<QueryBatchResponse> {
        let responses = self
            .qdrant_client
            .query_batch(QueryBatchPointsBuilder::new(self.collection_name, searches))
            .await?;

        Ok(responses)
    }

    pub async fn insert(&self, point: PointStruct) -> Result<()> {
        self.qdrant_client
            .upsert_points(UpsertPointsBuilder::new(self.collection_name, vec![point]).wait(true))
            .await?;

        Ok(())
    }

    pub async fn insert_many(
        &self,
        point_structs: Vec<PointStruct>,
        chunk_size: usize,
    ) -> Result<()> {
        if point_structs.is_empty() {
            return Ok(());
        }

        let chunk_size = if chunk_size < 2_000 {
            2_000
        } else {
            chunk_size
        };

        self.qdrant_client
            .upsert_points_chunked(
                UpsertPointsBuilder::new(self.collection_name, point_structs),
                chunk_size,
            )
            .await
            .context("failed to insert_many of points")?;

        Ok(())
    }

    pub async fn insert_and_search<T>(
        &self,
        point_data: T,
        score_threshold: f32,
        filter: Option<Filter>,
    ) -> Result<()>
    where
        T: TryInto<models::QdrantPointData>,
        T::Error: Into<anyhow::Error>,
    {
        let point_data = point_data.try_into().map_err(Into::into)?;
        let anchor = point_data.get_payload();
        let (point, vec_data) = self.create_point(point_data).await?;

        self.insert(point).await?;

        let response = self
            .semantic_similarity_search(vec_data, score_threshold, 10, filter, true)
            .await?;

        if response.result.len() == 0 {
            return Ok(());
        }

        let clone_anchor = anchor.clone();
        let hit = protos::SimilarityHit::try_from_results(anchor, response.result)?;

        self.tx
            .send(protos::Ebo {
                correlation_id: hex::encode(Sha256::digest(format!(
                    "{}:{}",
                    clone_anchor.uuid, clone_anchor.market_id
                ))),
                arb_found_at: chrono::Utc::now().timestamp_millis(),
                action: Some(protos::ebo::Action::CrossPlatformArb(hit)),
            })
            .await
            .context("error sending to channel")?;
        Ok(())
    }

    pub async fn insert_many_and_search<T>(
        &self,
        values: Vec<(T, Option<Filter>)>,
        chunk_size: usize,
    ) -> Result<()>
    where
        T: TryInto<models::QdrantPointData>,
        T::Error: Into<anyhow::Error>,
    {
        let (converted_data, filters): (Vec<models::QdrantPointData>, Vec<Option<Filter>>) = values
            .into_iter()
            .map(|(t, f)| {
                let data = t.try_into().map_err(Into::into)?;
                Ok((data, f))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .unzip();

        let takes: Vec<protos::QdrantPayload> = converted_data
            .iter()
            .map(|d| d.get_payload().clone())
            .collect();

        let points_and_vectors = self.create_points(converted_data).await?;

        let (point_structs, vectors): (Vec<PointStruct>, Vec<Vec<f32>>) =
            points_and_vectors.into_iter().unzip();

        self.insert_many(point_structs, chunk_size).await?;

        let mut query_points = Vec::with_capacity(vectors.len());
        for (vec_data, filter) in vectors.into_iter().zip(filters) {
            let mut request = QueryPointsBuilder::new(self.collection_name)
                .query(Query::new_nearest(vec_data))
                .score_threshold(SIMILARITY_SCORE_THRESHOLD)
                .limit(10)
                .with_payload(true);

            if let Some(f) = filter {
                request = request.filter(f);
            }

            query_points.push(request.build());
        }

        let responses = self.semantic_similarity_search_batch(query_points).await?;

        if responses.result.len() == 0 {
            return Ok(());
        }

        if takes.len() != responses.result.len() {
            anyhow::bail!(
                "batch search alignment error: Sent {} queries but received {} responses",
                takes.len(),
                responses.result.len()
            );
        }

        for (take, response) in takes.into_iter().zip(responses.result) {
            if response.result.is_empty() {
                continue;
            }

            let clone_take = take.clone();
            let hit = protos::SimilarityHit::try_from_results(take, response.result)?;

            self.tx
                .send(protos::Ebo {
                    correlation_id: hex::encode(Sha256::digest(format!(
                        "{}:{}",
                        clone_take.uuid, clone_take.market_id
                    ))),
                    arb_found_at: chrono::Utc::now().timestamp_millis(),
                    action: Some(protos::ebo::Action::CrossPlatformArb(hit)),
                })
                .await
                .context("error sending to channel")?;
        }

        Ok(())
    }

    pub async fn delete_points(&self, ids: Vec<PointId>) -> Result<()> {
        self.qdrant_client
            .delete_points(
                DeletePointsBuilder::new("{collection_name}")
                    .points(PointsIdsList { ids: ids })
                    .wait(true),
            )
            .await?;

        Ok(())
    }

    pub async fn create_points<T>(&self, data_list: Vec<T>) -> Result<Vec<(PointStruct, Vec<f32>)>>
    where
        T: TryInto<models::QdrantPointData>,
        T::Error: Into<anyhow::Error>,
    {
        if data_list.is_empty() {
            return Ok(vec![]);
        }

        let data_list: Vec<models::QdrantPointData> = data_list
            .into_iter()
            .map(|t| t.try_into().map_err(Into::into))
            .collect::<Result<Vec<_>>>()?;

        let texts: Vec<String> = data_list
            .iter()
            .map(|d| d.get_question_vector().to_owned())
            .collect();

        let model = self.semantic_model.clone();
        let embeddings: Vec<Vec<f32>> = task::spawn_blocking(move || {
            let refs: Vec<&str> = texts.iter().map(|s| s.as_str()).collect();
            model.embed(&refs)
        })
        .await
        .context("embedding task panicked")??;

        let mut result: Vec<(PointStruct, Vec<f32>)> = Vec::with_capacity(data_list.len());
        for (data, vector_data) in data_list.into_iter().zip(embeddings) {
            let mut point = PointStruct::try_from(data)?;
            point.vectors = Some((&vector_data[..]).into());
            result.push((point, vector_data));
        }

        Ok(result)
    }

    pub async fn create_point<T>(&self, data: T) -> Result<(PointStruct, Vec<f32>)>
    where
        T: TryInto<models::QdrantPointData>,
        T::Error: Into<anyhow::Error>,
    {
        let data = data.try_into().map_err(Into::into)?;
        let text = data.get_question_vector().to_string();

        let model = self.semantic_model.clone();
        let embedding = tokio::task::spawn_blocking(move || model.embed(&[&text])).await??;

        let vec_data = embedding
            .into_iter()
            .next()
            .context("embedding model returned no vectors")?;

        let mut point = PointStruct::try_from(data)?;
        point.vectors = Some((&vec_data[..]).into());

        Ok((point, vec_data))
    }

    pub fn create_cross_platform_filter(
        platform: Option<String>,
        market: &models::MarketTag,
    ) -> Option<Filter> {
        let platform = platform?;

        Some(Filter::must([
            Condition::matches(FIELD_PLATFORM, !MatchValue::Keyword(platform)),
            Condition::matches(FIELD_MARKET_CATEGORY, market.info().category.to_string()),
            Condition::matches(
                FIELD_MARKET_SUBCATEGORY,
                market.info().subcategory.to_string(),
            ),
        ]))
    }

    pub fn create_intra_platform_filter(
        platform: Option<String>,
        market: &models::MarketTag,
    ) -> Option<Filter> {
        let platform = platform?;

        Some(Filter::must([
            Condition::matches(FIELD_PLATFORM, platform),
            Condition::matches(FIELD_MARKET_CATEGORY, market.info().category.to_string()),
            Condition::matches(
                FIELD_MARKET_SUBCATEGORY,
                market.info().subcategory.to_string(),
            ),
        ]))
    }

    pub async fn get_last_insert_time(&self, platform_name: &str) -> Result<Option<DateTime<Utc>>> {
        let response = self
            .qdrant_client
            .scroll(
                ScrollPointsBuilder::new(COLLECTION_NAME)
                    .filter(Filter::must([Condition::matches(
                        FIELD_PLATFORM,
                        platform_name.to_string(),
                    )]))
                    .limit(1)
                    .with_payload(true)
                    .order_by(OrderBy {
                        key: FIELD_INSERTED_AT.to_string(),
                        direction: Some(Direction::Desc.into()),
                        ..Default::default()
                    }),
            )
            .await
            .context("failed to scroll Qdrant for last insert time")?;

        if let Some(point) = response.result.first() {
            if let Some(value) = point.payload.get(FIELD_INSERTED_AT) {
                if let Some(qdrant_client::qdrant::value::Kind::IntegerValue(ts_millis)) =
                    &value.kind
                {
                    let date = DateTime::from_timestamp_millis(*ts_millis)
                        .context("invalid timestamp in vectorDB")?;

                    return Ok(Some(date));
                }
            }
        }

        Ok(None)
    }
}
