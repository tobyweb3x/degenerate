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

func (r *Repository) InsertNewCrossHit(ctx context.Context, correctionId string, timestamp time.Time, similarityHit []byte) error {
	return r.dbQuery.InsertNewSimilarityHit(ctx, postgres.InsertNewSimilarityHitParams{
		CorrelationID: correctionId,
		FoundAt: pgtype.Timestamptz{
			Time:             timestamp,
			InfinityModifier: pgtype.Finite,
			Valid:            true,
		},
		SimilarityHit: similarityHit,
		ArbType:       "cross",
	})
}

func (r *Repository) InsertNewCrossArb(ctx context.Context, correctionId string, timestamp time.Time, arb []byte) error {
	return r.dbQuery.InsertNewArb(ctx, postgres.InsertNewArbParams{
		CorrelationID: correctionId,
		FoundAt: pgtype.Timestamptz{
			Time:             timestamp,
			InfinityModifier: pgtype.Finite,
			Valid:            true,
		},
		Arbs:    arb,
		ArbType: "cross",
	})
}

func (r *Repository) InsertNewIntraArb(ctx context.Context, correctionId string, timestamp time.Time, arb []byte) error {
	return r.dbQuery.InsertNewArb(ctx, postgres.InsertNewArbParams{
		CorrelationID: correctionId,
		FoundAt: pgtype.Timestamptz{
			Time:             timestamp,
			InfinityModifier: pgtype.Finite,
			Valid:            true,
		},
		Arbs:    arb,
		ArbType: "intra",
	})
}

func (r *Repository) InsertNewIntraHit(ctx context.Context, correctionId string, timestamp time.Time, similarityHit []byte) error {
	return r.dbQuery.InsertNewSimilarityHit(ctx, postgres.InsertNewSimilarityHitParams{
		CorrelationID: correctionId,
		FoundAt: pgtype.Timestamptz{
			Time:             timestamp,
			InfinityModifier: pgtype.Finite,
			Valid:            true,
		},
		SimilarityHit: similarityHit,
		ArbType:       "intra",
	})
}

func (r *Repository) GetRecentCrossHits(ctx context.Context) ([]postgres.SimilarityHit, error) {
	return r.dbQuery.GetRecentCrossHits(ctx, 5_000)
}

func (r *Repository) GetRecentCrossArbs(ctx context.Context) ([]postgres.Arb, error) {
	return r.dbQuery.GetRecentCrossArbs(ctx, 5_000)
}

func (r *Repository) GetSimilarityHitByCorrelationID(ctx context.Context, correctionId string) (postgres.SimilarityHit, error) {
	hit, err := r.dbQuery.GetSimilarityHitByCorrelationID(ctx, correctionId)
	if err != nil {
		return postgres.SimilarityHit{}, err
	}

	return hit, nil
}

func (r *Repository) GetArbByCorrelationID(ctx context.Context, correctionId string) (postgres.Arb, error) {
	hit, err := r.dbQuery.GetArbByCorrelationID(ctx, correctionId)
	if err != nil {
		return postgres.Arb{}, err
	}

	return hit, nil
}

func (r *Repository) BulkInsertSimilarityHits(ctx context.Context, bulk ...postgres.BulkInsertSimilarityHitsParams) (int64, error) {
	return r.dbQuery.BulkInsertSimilarityHits(ctx, bulk)
}

func (r *Repository) DeleteSimilarityHit(ctx context.Context, correctionID string) error {
	return r.dbQuery.DeleteSimilarityHit(ctx, correctionID)
}

func (r *Repository) DeleteArb(ctx context.Context, correctionID string) error {
	return r.dbQuery.DeleteArb(ctx, correctionID)
}

func (r *Repository) UpdateArbConfirm(ctx context.Context, param postgres.UpdateArbConfirmParams) error {
	return r.dbQuery.UpdateArbConfirm(ctx, param)
}

func (r *Repository) UpdateArbRunning(ctx context.Context, param postgres.UpdateArbRunningParams) error {
	return r.dbQuery.UpdateArbRunning(ctx, param)
}
