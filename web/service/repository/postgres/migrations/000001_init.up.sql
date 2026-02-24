
CREATE TABLE IF NOT EXISTS needs_resolve (
    id BIGSERIAL PRIMARY KEY,
    correlation_id TEXT NOT NULL UNIQUE,
    "timestamp" TIMESTAMPTZ NOT NULL,
    similarity_hit JSONB NOT NULL,
    arb_type TEXT NOT NULL CHECK (arb_type IN ('cross', 'intra')),
    created_at TIMESTAMPTZ DEFAULT NOW()
);
