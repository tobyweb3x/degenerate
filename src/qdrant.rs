use crate::models;
use crate::models::QdrantPointData;
use anyhow::{Context, Ok, Result};
use candle_core::Device;
use qdrant_client::{
    Payload, Qdrant,
    qdrant::{
         CreateCollectionBuilder, CreateFieldIndexCollectionBuilder, DeletePointsBuilder,
        FieldType, Filter, HnswConfigDiffBuilder, PointId, PointStruct, PointsIdsList, Query,
        QueryBatchPointsBuilder, QueryBatchResponse, QueryPoints, QueryPointsBuilder,
        QueryResponse, UpdateCollectionBuilder, UpsertPointsBuilder, VectorInput,
        VectorParamsBuilder,
    },
};
use sentence_transformers_rs::sentence_transformer::{
    SentenceTransformer, SentenceTransformerBuilder, Which,
};
use serde_json::json;
use std::convert::TryFrom;
use std::sync::Arc;
use tokio::task;

#[derive(Clone)]
pub struct VectorStore {
    qudrant_client: Qdrant,
    collection_name: &'static str,
    semantic_model: Arc<SentenceTransformer>,
}

impl VectorStore {
    pub async fn new_metal(url: &str, collection_name: &'static str) -> Result<Self> {
        let device =
            candle_core::Device::new_metal(0).context("metal device initialization failed")?;

        Self::new(url, collection_name, device).await
    }

    async fn new(url: &str, collection_name: &'static str, device: Device) -> Result<Self> {
        let client = Qdrant::from_url(url).build()?;

        if !client.collection_exists(collection_name).await? {
            client
                .create_collection(
                    CreateCollectionBuilder::new(collection_name).vectors_config(
                        VectorParamsBuilder::new(384, qdrant_client::qdrant::Distance::Cosine),
                    ),
                )
                .await?;
        }

        let model = SentenceTransformerBuilder::with_sentence_transformer(&Which::AllMiniLML6v2)
            .batch_size(2048)
            .with_device(&device)
            .build()
            .context("sentenceTransformer build failed")?;

        Ok(Self {
            qudrant_client: client,
            collection_name,
            semantic_model: Arc::new(model),
        })
    }

    pub async fn disable_hnsw(&self) -> Result<()> {
        self.qudrant_client
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

        self.qudrant_client
            .update_collection(
                UpdateCollectionBuilder::new(self.collection_name)
                    .hnsw_config(HnswConfigDiffBuilder::default().m(edges_per_node)),
            )
            .await
            .context("failed to enable HNSW indexing")?;

        Ok(())
    }

    pub async fn index_payload(&self, field_name: &str, field_type: FieldType) -> Result<()> {
        if field_name.is_empty() {
            return Ok(());
        }
        self.qudrant_client
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

        let response = self.qudrant_client.query(request.build()).await?;

        Ok(response)
    }

    pub async fn semantic_similarity_search_batch(
        &self,
        searches: Vec<QueryPoints>,
    ) -> Result<QueryBatchResponse> {
        let responses = self
            .qudrant_client
            .query_batch(QueryBatchPointsBuilder::new(self.collection_name, searches))
            .await?;

        Ok(responses)
    }

    pub async fn insert(&self, point: PointStruct) -> Result<()> {
        self.qudrant_client
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

        let len = point_structs.len();
        let chunk_size = if chunk_size < 1_000 {
            1_000
        } else {
            chunk_size
        };

        self.qudrant_client
            .upsert_points_chunked(
                UpsertPointsBuilder::new(self.collection_name, point_structs),
                chunk_size,
            )
            .await
            .context("failed to insert_many of points")?;

        Ok(())
    }

    pub async fn insert_and_search(
        &self,
        point_data: models::QdrantPointData,
        score_threshold: f32,
        filter: Option<Filter>,
    ) -> Result<QueryResponse> {
        let (point, vec_data) = self.create_point(point_data).await?;
        self.insert(point).await?;
        self.semantic_similarity_search(vec_data, score_threshold, 10, filter, true)
            .await
    }

    pub async fn insert_many_and_search(
        &self,
        values: Vec<(models::QdrantPointData, Option<Filter>)>,
        chunk_size: usize,
    ) -> Result<QueryBatchResponse> {
        let (point_structs, filters): (Vec<QdrantPointData>, Vec<Option<Filter>>) =
            values.into_iter().unzip();
        let result = self.create_points(point_structs).await?;
        let (point_structs, vec_datas): (Vec<PointStruct>, Vec<Vec<f32>>) =
            result.into_iter().unzip();
        self.insert_many(point_structs, chunk_size).await?;

        let mut query_points = Vec::with_capacity(vec_datas.len());
        for (vec_data, filter) in vec_datas.into_iter().zip(filters) {
            let mut request = QueryPointsBuilder::new(self.collection_name)
                .query(Query::new_nearest(vec_data))
                .score_threshold(0.85)
                .limit(10)
                .with_payload(true);

            if let Some(f) = filter {
                request = request.filter(f);
            }

            query_points.push(request.build());
        }

        self.semantic_similarity_search_batch(query_points).await
    }

    pub async fn delete_points(&self, ids: Vec<PointId>) -> Result<()> {
        self.qudrant_client
            .delete_points(
                DeletePointsBuilder::new("{collection_name}")
                    .points(PointsIdsList { ids: ids })
                    .wait(true),
            )
            .await?;

        Ok(())
    }

    pub async fn create_points(
        &self,
        data_list: Vec<QdrantPointData>,
    ) -> Result<Vec<(PointStruct, Vec<f32>)>> {
        if data_list.is_empty() {
            return Ok(vec![]);
        }

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

    pub async fn create_point(&self, data: QdrantPointData) -> Result<(PointStruct, Vec<f32>)> {
        let model = self.semantic_model.clone();
        let text = data.get_question_vector().to_string();

        let embedding = tokio::task::spawn_blocking(move || model.embed(&[&text])).await??;

        let vec_data = embedding
            .into_iter()
            .next()
            .context("embedding model returned no vectors")?;

        let mut point = PointStruct::try_from(data)?;
        point.vectors = Some((&vec_data[..]).into());

        Ok((point, vec_data))
    }
}

impl TryFrom<QdrantPointData> for PointStruct {
    type Error = anyhow::Error;
    fn try_from(value: QdrantPointData) -> Result<Self, Self::Error> {
        let payload =
            Payload::try_from(json!(value.get_payload())).context("payload conversion failed")?;

        Ok(Self {
            payload: payload.into(),
            id: Some(PointId::from(value.get_id())),
            ..Default::default()
        })
    }
}
