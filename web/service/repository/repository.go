package repository

import (
	"context"
	"time"
	"web/service/repository/postgres"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type Repository struct {
	dbQuery *postgres.Queries
	pgxPool PostgresDb
}

type PostgresDb interface {
	postgres.DBTX
	Begin(ctx context.Context) (pgx.Tx, error)
}

func NewService(postgresDb PostgresDb) *Repository {
	var s Repository
	s.dbQuery = postgres.New(postgresDb)
	s.pgxPool = postgresDb
	return &s
}

func (r *Repository) InsertNewNeedsResolveCrossArbs(ctx context.Context, correctionId string, timestamp time.Time, similarityHit []byte) error {
	return r.dbQuery.InsertNeedsResolve(ctx, postgres.InsertNeedsResolveParams{
		CorrelationID: correctionId,
		ArbFoundAt: pgtype.Timestamptz{
			Time:             timestamp,
			InfinityModifier: pgtype.Finite,
			Valid:            true,
		},
		SimilarityHit: similarityHit,
		ArbType:       "cross",
	})
}

func (r *Repository) InsertNewNeedsResolvedIntraArbs(ctx context.Context, correctionId string, timestamp time.Time, similarityHit []byte) error {
	return r.dbQuery.InsertNeedsResolve(ctx, postgres.InsertNeedsResolveParams{
		CorrelationID: correctionId,
		ArbFoundAt: pgtype.Timestamptz{
			Time:             timestamp,
			InfinityModifier: pgtype.Finite,
			Valid:            true,
		},
		SimilarityHit: similarityHit,
		ArbType:       "intra",
	})
}

func (r *Repository) GetAllCrossArbs(ctx context.Context) ([]postgres.NeedsResolve, error) {
	return r.dbQuery.GetRecentCrossArbs(ctx, 1_000)
}

func (r *Repository) GetNeedsResolveByCorrelationID(ctx context.Context, correctionId string) (postgres.NeedsResolve, error) {
	needsResolved, err := r.dbQuery.GetNeedsResolveByCorrelationID(ctx, correctionId)
	if err != nil {
		return postgres.NeedsResolve{}, err
	}

	return needsResolved, nil
}

func (r *Repository) BulkInsertNewNeedsResolve(ctx context.Context, bulk ...postgres.BulkInsertNeedsResolveParams) (int64, error) {
	return r.dbQuery.BulkInsertNeedsResolve(ctx, bulk)
}

func (r *Repository) DeleteNeedsResolve(ctx context.Context, correctionID string) error {
	return r.dbQuery.DeleteNeedsResolve(ctx, correctionID)
}
