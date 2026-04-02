-- name: InsertNewSimilarityHit :exec
INSERT INTO similarity_hits (
    correlation_id,
    found_at,
    similarity_hit,
    arb_type
)
VALUES ($1, $2, $3, $4)
ON CONFLICT (correlation_id) DO NOTHING;

-- name: InsertNewArb :exec
INSERT INTO arbs (
    correlation_id,
    found_at,
    arbs,
    arb_type
)
VALUES ($1, $2, $3, $4)
ON CONFLICT (correlation_id) DO NOTHING;

-- name: BulkInsertSimilarityHits :copyfrom
INSERT INTO similarity_hits (
    correlation_id, found_at, similarity_hit, arb_type
) VALUES (
    $1, $2, $3, $4
);

-- name: GetSimilarityHitByCorrelationID :one
SELECT *
FROM similarity_hits
WHERE correlation_id = $1 
    AND is_deleted = FALSE;

-- name: GetArbByCorrelationID :one
SELECT *
FROM arbs
WHERE correlation_id = $1;

-- name: GetRecentHits :many
SELECT *
FROM similarity_hits
WHERE arb_type = $1
  AND is_deleted = FALSE
ORDER BY created_at DESC
LIMIT $2;

-- name: GetRecentArbs :many
SELECT *
FROM arbs
WHERE arb_type = $1
ORDER BY created_at DESC
LIMIT $2;

-- name: GetRunninngArbs :many
SELECT *
FROM arbs
WHERE arb_type = $1 AND running = TRUE
ORDER BY created_at DESC;

-- name: DeleteSimilarityHit :exec
UPDATE similarity_hits
SET is_deleted = TRUE,
    deleted_at = NOW()
WHERE correlation_id = $1;

-- name: DeleteArb :exec
DELETE FROM arbs
WHERE correlation_id = $1;

-- name: DeleteSimilarityHitsBulk :exec
UPDATE similarity_hits
SET is_deleted = TRUE,
    deleted_at = NOW()
WHERE correlation_id = ANY(sqlc.arg(correlation_ids)::text[]);

-- name: DeleteArbsBulk :exec
DELETE FROM arbs
WHERE correlation_id = ANY(sqlc.arg(correlation_ids)::text[]);

-- name: UpdateArbConfirm :exec
UPDATE arbs
SET confirmed = $2
WHERE correlation_id = $1;

-- name: UpdateArbRunning :exec
UPDATE arbs
SET running = $2
WHERE correlation_id = $1;

-- name: UpdateArbStatus :one
UPDATE arbs
SET confirmed = $2,
    running = $3
WHERE correlation_id = $1
RETURNING *;

-- name: HardDeleteOldSimilarityHits :exec
DELETE FROM similarity_hits
WHERE is_deleted = TRUE AND deleted_at < $1;