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
WHERE correlation_id = $1;

-- name: GetArbByCorrelationID :one
SELECT *
FROM arbs
WHERE correlation_id = $1;

-- name: GetRecentCrossHits :many
SELECT *
FROM similarity_hits
WHERE arb_type = 'cross'
ORDER BY created_at DESC
LIMIT $1;

-- name: GetRecentCrossArbs :many
SELECT *
FROM arbs
WHERE arb_type = 'cross'
ORDER BY created_at DESC
LIMIT $1;

-- name: GetRecentIntraHits :many
SELECT *
FROM similarity_hits
WHERE arb_type = 'intra'
ORDER BY created_at DESC
LIMIT $1;

-- name: GetRecentIntraArbs :many
SELECT *
FROM arbs
WHERE arb_type = 'intra'
ORDER BY created_at DESC
LIMIT $1;

-- name: DeleteSimilarityHit :exec
DELETE FROM similarity_hits
WHERE correlation_id = $1;

-- name: DeleteArb :exec
DELETE FROM arbs
WHERE correlation_id = $1;

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