
CREATE TABLE IF NOT EXISTS similarity_hits (
    id BIGSERIAL PRIMARY KEY,
    correlation_id TEXT NOT NULL UNIQUE,
    found_at TIMESTAMPTZ NOT NULL,
    similarity_hit JSONB NOT NULL,
    arb_type TEXT NOT NULL CHECK (arb_type IN ('cross', 'intra')),
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS arbs (
    id BIGSERIAL PRIMARY KEY,
    correlation_id TEXT NOT NULL UNIQUE,
    found_at TIMESTAMPTZ NOT NULL,
    arbs JSONB NOT NULL,
    confirmed BOOLEAN NOT NULL DEFAULT FALSE,
    running BOOLEAN NOT NULL DEFAULT FALSE,
    arb_type TEXT NOT NULL CHECK (arb_type IN ('cross', 'intra')),
    created_at TIMESTAMPTZ DEFAULT NOW()
);
