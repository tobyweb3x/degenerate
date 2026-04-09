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

-- name: InsertOrder :exec
INSERT INTO orders (
    correlation_id, 
    found_at, 
    arbs, 
    anchor_cost, 
    match_cost, 
    anchor_fill, 
    match_fill, 
    excess_fill, 
    anchor_order_id, 
    match_order_id
) VALUES (
    sqlc.arg('order_correlation_id'),
    sqlc.arg('found_at'),
    (SELECT arbs.arbs FROM arbs WHERE arbs.correlation_id = sqlc.arg('arb_correlation_id')),
    sqlc.arg('anchor_cost'),
    sqlc.arg('match_cost'),
    sqlc.arg('anchor_fill'),
    sqlc.arg('match_fill'),
    sqlc.arg('excess_fill'),
    sqlc.arg('anchor_order_id'),
    sqlc.arg('match_order_id')
);

-- name: GetOrderByCorrelationID :one
SELECT *
FROM orders
WHERE correlation_id = $1;

-- name: GetAllOrders :many
SELECT *
FROM orders
ORDER BY found_at DESC;

-- name: InsertExcessFill :exec
INSERT INTO excess_fill (
    correlation_id,
    found_at,
    platform,
    order_id,
    fill_size,
    fill_cost
) VALUES (
    $1, $2, $3, $4, $5, $6
);

-- name: GetExcessFillByCorrelationID :one
SELECT *
FROM excess_fill
WHERE correlation_id = $1;

-- name: GetAllExcessFills :many
SELECT *
FROM excess_fill
ORDER BY found_at DESC;

-- name: GetOrderWithExcess :many
SELECT 
    o.correlation_id,
    o.found_at,
    o.arbs,
    o.anchor_cost,
    o.match_cost,
    o.anchor_fill,
    o.match_fill,
    o.excess_fill,
    o.anchor_order_id,
    o.match_order_id,
    e.platform AS hedge_platform,
    e.order_id AS hedge_order_id,
    e.fill_size AS hedge_fill_size,
    e.fill_cost AS hedge_fill_cost
FROM orders o
LEFT JOIN excess_fill e ON o.correlation_id = e.correlation_id
ORDER BY o.found_at DESC
LIMIT $1;