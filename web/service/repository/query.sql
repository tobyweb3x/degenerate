-- name: InsertNeedsResolve :exec
INSERT INTO needs_resolve (
    correlation_id,
    arb_found_at,
    similarity_hit,
    arb_type
)
VALUES ($1, $2, $3, $4)
ON CONFLICT (correlation_id) DO NOTHING;

-- name: BulkInsertNeedsResolve :copyfrom
INSERT INTO needs_resolve (
    correlation_id, arb_found_at, similarity_hit, arb_type
) VALUES (
    $1, $2, $3, $4
);

-- name: GetNeedsResolveByCorrelationID :one
SELECT *
FROM needs_resolve
WHERE correlation_id = $1;

-- name: GetRecentCrossArbs :many
SELECT *
FROM needs_resolve
WHERE arb_type = 'cross'
ORDER BY created_at DESC
LIMIT $1;

-- name: GetRecentIntraArbs :many
SELECT *
FROM needs_resolve
WHERE arb_type = 'intra'
ORDER BY created_at DESC
LIMIT $1;

-- name: DeleteNeedsResolve :exec
DELETE FROM needs_resolve
WHERE correlation_id = $1;