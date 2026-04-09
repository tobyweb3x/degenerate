use crate::models::{self, protos};
use anyhow::Context;
use candle_core::Device;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use qdrant_client::{
    Qdrant,
    qdrant::{
        Condition, CreateCollectionBuilder, CreateFieldIndexCollectionBuilder, DeletePointsBuilder,
        Direction, FieldType, Filter, HnswConfigDiffBuilder, OrderBy, PointStruct, Query,
        QueryBatchPointsBuilder, QueryBatchResponse, QueryPoints, QueryPointsBuilder,
        QueryResponse, Range, ScrollPointsBuilder, UpdateCollectionBuilder, UpsertPointsBuilder,
        VectorInput, VectorParamsBuilder, r#match::MatchValue,
    },
};
use sentence_transformers_rs::sentence_transformer::{
    SentenceTransformer, SentenceTransformerBuilder, Which,
};
use tokio_util::sync::CancellationToken;

use std::convert::TryFrom;
use std::sync::Arc;
use tokio::{sync::mpsc, task};

pub const COLLECTION_NAME: &str = "Aroni";

// Payload indexed Field Names
pub const FIELD_UUID: &str = "market_info.uuid";
pub const FIELD_MARKET_CATEGORY: &str = "market_info.market_category";
pub const FIELD_PLATFORM: &str = "market_info.platform";
pub const FIELD_MARKET_SUBCATEGORY: &str = "market_info.market_subcategory";
pub const FIELD_CLOSE_TIME_MS: &str = "market_info.close_time_ms";
pub const FIELD_INSERTED_AT: &str = "qdrant_inserted_at";

pub const SIMILARITY_SCORE_THRESHOLD: f32 = 0.75;

#[derive(Clone)]
pub struct VectorStore {
    qdrant_client: Qdrant,
    collection_name: &'static str,
    semantic_model: Arc<SentenceTransformer>,
    tx: mpsc::Sender<protos::ClientEbo>,
}

impl VectorStore {
    pub async fn new_auto(
        url: &str,
        collection_name: &'static str,
        tx: mpsc::Sender<protos::ClientEbo>,
    ) -> anyhow::Result<Self> {
        let device = candle_core::Device::metal_if_available(0).context("error using metal")?;
        let vs = Self::new(url, collection_name, device, tx).await?;
        tracing::info!("successfully setup vector store");
        Ok(vs)
    }

    async fn new(
        url: &str,
        collection_name: &'static str,
        device: Device,
        tx: mpsc::Sender<protos::ClientEbo>,
    ) -> anyhow::Result<Self> {
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

    pub async fn disable_hnsw(&self) -> anyhow::Result<()> {
        self.qdrant_client
            .update_collection(
                UpdateCollectionBuilder::new(self.collection_name)
                    .hnsw_config(HnswConfigDiffBuilder::default().m(0)),
            )
            .await
            .context("failed to disable HNSW indexing")?;

        anyhow::Ok(())
    }

    pub async fn enable_hnsw(&self, edges_per_node: u64) -> anyhow::Result<()> {
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
        anyhow::Ok(())
    }

    async fn index_payload(&self, field_name: &str, field_type: FieldType) -> anyhow::Result<()> {
        if field_name.is_empty() {
            return anyhow::Ok(());
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

        anyhow::Ok(())
    }

    pub async fn setup_qdrant_payload_index(&self) -> anyhow::Result<()> {
        self.index_payload(FIELD_UUID, FieldType::Uuid)
            .await
            .context(format!("qdrant index error on {}", FIELD_UUID))?;

        self.index_payload(FIELD_PLATFORM, FieldType::Integer)
            .await
            .context(format!("qdrant index error on {}", FIELD_PLATFORM))?;

        self.index_payload(FIELD_MARKET_CATEGORY, FieldType::Keyword)
            .await
            .context(format!("qdrant index error on {}", FIELD_MARKET_CATEGORY))?;

        self.index_payload(FIELD_MARKET_SUBCATEGORY, FieldType::Keyword)
            .await
            .context(format!("qdrant index error on {}", FIELD_MARKET_CATEGORY))?;

        self.index_payload(FIELD_CLOSE_TIME_MS, FieldType::Integer)
            .await
            .context(format!("qdrant index error on {}", FIELD_CLOSE_TIME_MS))?;

        self.index_payload(FIELD_INSERTED_AT, FieldType::Integer) // Changed
            .await
            .context(format!("qdrant index error on {}", FIELD_INSERTED_AT))?;

        anyhow::Ok(())
    }

    pub async fn semantic_similarity_search(
        &self,
        vector: impl Into<VectorInput>,
        score_threshold: f32,
        limit: u64,
        filter: Option<Filter>,
        with_paylod: bool,
    ) -> anyhow::Result<QueryResponse> {
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
    ) -> anyhow::Result<QueryBatchResponse> {
        let responses = self
            .qdrant_client
            .query_batch(QueryBatchPointsBuilder::new(self.collection_name, searches))
            .await?;

        Ok(responses)
    }

    pub async fn insert(&self, point: PointStruct) -> anyhow::Result<()> {
        self.qdrant_client
            .upsert_points(UpsertPointsBuilder::new(self.collection_name, vec![point]).wait(true))
            .await?;

        anyhow::Ok(())
    }

    pub async fn insert_many(
        &self,
        point_structs: Vec<PointStruct>,
        chunk_size: usize,
    ) -> anyhow::Result<()> {
        if point_structs.is_empty() {
            return anyhow::Ok(());
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

        anyhow::Ok(())
    }

    pub async fn search_and_insert<T>(
        &self,
        point_data: T,
        score_threshold: f32,
        filter: Option<Filter>,
    ) -> anyhow::Result<()>
    where
        T: TryInto<models::QdrantPointData>,
        T::Error: Into<anyhow::Error>,
    {
        let point_data = point_data.try_into().map_err(Into::into)?;
        let anchor = point_data.get_payload();
        let (point, vec_data) = self.create_point(point_data).await?;

        // search
        let response = self
            .semantic_similarity_search(vec_data, score_threshold, 10, filter, true)
            .await?;

        if response.result.is_empty() {
            return anyhow::Ok(());
        }

        let clone_anchor_market_info = anchor
            .clone()
            .market_info
            .context("market should not be None")?;
        let hit = protos::SimilarityHit::try_from_results(anchor, response.result)?;

        // insert
        self.insert(point).await?;

        self.tx
            .send(protos::ClientEbo {
                correlation_id: models::generate_uuid_v5(format!(
                    "{}:{}",
                    clone_anchor_market_info.uuid, clone_anchor_market_info.market_id
                )),
                action_at: chrono::Utc::now().timestamp_millis(),
                action: Some(protos::client_ebo::Action::CrossPlatformHit(hit)),
            })
            .await
            .context("error sending to channel")?;
        anyhow::Ok(())
    }

    pub async fn multiple_search_and_inserth<T>(
        &self,
        values: Vec<(T, Option<Filter>)>,
        chunk_size: usize,
    ) -> anyhow::Result<()>
    where
        T: TryInto<models::QdrantPointData>,
        T::Error: Into<anyhow::Error>,
    {
        let (converted_data, filters): (Vec<models::QdrantPointData>, Vec<Option<Filter>>) = values
            .into_iter()
            .filter_map(|(t, f)| match t.try_into().map_err(Into::into) {
                Ok(data) => Some((data, f)),
                Err(e) => {
                    tracing::warn!("error creating new QdrantPointData(dropped): {e:#?}");
                    None
                }
            })
            .unzip();

        let takes: Vec<protos::QdrantPayload> = converted_data
            .iter()
            .map(|d| d.get_payload().clone())
            .collect();

        let points_and_vectors = self.create_points(converted_data).await?;

        let (point_structs, vectors): (Vec<PointStruct>, Vec<Vec<f32>>) =
            points_and_vectors.into_iter().unzip();

        // search
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

        if responses.result.is_empty() {
            return anyhow::Ok(());
        }

        if takes.len() != responses.result.len() {
            anyhow::bail!(
                "batch search alignment error: Sent {} queries but received {} responses",
                takes.len(),
                responses.result.len()
            );
        }

        // insert
        self.insert_many(point_structs, chunk_size).await?;

        for (take, response) in takes.into_iter().zip(responses.result) {
            if response.result.is_empty() {
                continue;
            }

            let clone_take_market_info = take
                .clone()
                .market_info
                .context("market_info should not be None")?;
            let hit = protos::SimilarityHit::try_from_results(take, response.result)?;

            self.tx
                .send(protos::ClientEbo {
                    correlation_id: models::generate_uuid_v5(format!(
                        "{}:{}",
                        clone_take_market_info.uuid, clone_take_market_info.market_id
                    )),
                    action_at: chrono::Utc::now().timestamp_millis(),
                    action: Some(protos::client_ebo::Action::CrossPlatformHit(hit)),
                })
                .await
                .context("error sending to channel")?;
        }

        anyhow::Ok(())
    }

    pub async fn run_delete_old_points(&self, shutdown: CancellationToken) -> anyhow::Result<()> {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(24 * 60 * 60));

        ticker.tick().await;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!(" Qdrant cleaner received shutdown signal");
                    break;
               }

                _ = ticker.tick() => {
                    let now = Utc::now();
                    let cutoff_time = now - ChronoDuration::days(2);
                    let cutoff_ms = cutoff_time.timestamp_millis();

                    let filter = Filter::must([Condition::range(
                        FIELD_CLOSE_TIME_MS,
                        Range {
                            lt: Some(cutoff_ms as f64),
                            gt: None,
                            gte: None,
                            lte: None,
                        },
                    )]);

                    match self
                        .qdrant_client
                        .delete_points(
                            DeletePointsBuilder::new(self.collection_name)
                                .points(filter)
                                .wait(true),
                        ).await {
                            Ok(_) => {
                                tracing::info!("✅ Qdrant Cleanup successful");
                            },
                            Err(e) => {
                                tracing::error!("failed to execute delete_points against Qdrant: {e:#?}");
                            }
                        }
                }

            }
        }

        Ok(())
    }

    pub async fn create_points<T>(
        &self,
        data_list: Vec<T>,
    ) -> anyhow::Result<Vec<(PointStruct, Vec<f32>)>>
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
            .collect::<anyhow::Result<Vec<_>>>()?;

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

    pub async fn create_point<T>(&self, data: T) -> anyhow::Result<(PointStruct, Vec<f32>)>
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
        platform: Option<protos::Platform>,
        market: &models::MarketTag,
    ) -> Option<Filter> {
        let platform = platform?;

        Some(Filter {
            must: vec![
                Condition::matches(
                    FIELD_MARKET_CATEGORY,
                    MatchValue::Keyword(market.info().market_category.to_string()),
                ),
                Condition::matches(
                    FIELD_MARKET_SUBCATEGORY,
                    MatchValue::Keyword(market.info().market_subcategory.to_string()),
                ),
            ],
            must_not: vec![Condition::matches(
                FIELD_PLATFORM,
                MatchValue::Integer(platform.into()),
            )],
            should: vec![],
            min_should: None,
        })
    }

    pub fn create_intra_platform_filter(
        platform: Option<protos::Platform>,
        market: &models::MarketTag,
    ) -> Option<Filter> {
        let platform = platform?;

        Some(Filter::must([
            Condition::matches(FIELD_PLATFORM, MatchValue::Integer(platform.into())),
            Condition::matches(
                FIELD_MARKET_CATEGORY,
                MatchValue::Keyword(market.info().market_category.to_string()),
            ),
            Condition::matches(
                FIELD_MARKET_SUBCATEGORY,
                MatchValue::Keyword(market.info().market_subcategory.to_string()),
            ),
        ]))
    }

    pub async fn get_last_insert_time(
        &self,
        platform_name: protos::Platform,
    ) -> anyhow::Result<Option<DateTime<Utc>>> {
        let response = self
            .qdrant_client
            .scroll(
                ScrollPointsBuilder::new(COLLECTION_NAME)
                    .filter(Filter::must([Condition::matches(
                        FIELD_PLATFORM,
                        MatchValue::Integer(platform_name.into()),
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

        if let Some(point) = response.result.first()
            && let Some(value) = point.payload.get(FIELD_INSERTED_AT)
            && let Some(qdrant_client::qdrant::value::Kind::IntegerValue(ts_millis)) = &value.kind
        {
            let date = DateTime::from_timestamp_millis(*ts_millis)
                .context("invalid timestamp in vectorDB")?;

            return Ok(Some(date));
        }

        Ok(None)
    }
}
