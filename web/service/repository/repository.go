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

type ArbType string

const (
	CrossArbType ArbType = "cross"
	IntraArbType ArbType = "intra"
)

func (r *Repository) InsertNewCrossHit(ctx context.Context, correlationId string, timestamp time.Time, similarityHit []byte) error {
	return r.dbQuery.InsertNewSimilarityHit(ctx, postgres.InsertNewSimilarityHitParams{
		CorrelationID: correlationId,
		FoundAt: pgtype.Timestamptz{
			Time:             timestamp,
			InfinityModifier: pgtype.Finite,
			Valid:            true,
		},
		SimilarityHit: similarityHit,
		ArbType:       "cross",
	})
}

func (r *Repository) InsertNewCrossArb(ctx context.Context, correlationId string, timestamp time.Time, arb []byte) error {
	return r.dbQuery.InsertNewArb(ctx, postgres.InsertNewArbParams{
		CorrelationID: correlationId,
		FoundAt: pgtype.Timestamptz{
			Time:             timestamp,
			InfinityModifier: pgtype.Finite,
			Valid:            true,
		},
		Arbs:    arb,
		ArbType: "cross",
	})
}

func (r *Repository) InsertNewIntraArb(ctx context.Context, correlationId string, timestamp time.Time, arb []byte) error {
	return r.dbQuery.InsertNewArb(ctx, postgres.InsertNewArbParams{
		CorrelationID: correlationId,
		FoundAt: pgtype.Timestamptz{
			Time:             timestamp,
			InfinityModifier: pgtype.Finite,
			Valid:            true,
		},
		Arbs:    arb,
		ArbType: "intra",
	})
}

func (r *Repository) InsertNewIntraHit(ctx context.Context, correlationId string, timestamp time.Time, similarityHit []byte) error {
	return r.dbQuery.InsertNewSimilarityHit(ctx, postgres.InsertNewSimilarityHitParams{
		CorrelationID: correlationId,
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
	return r.dbQuery.GetRecentHits(ctx, postgres.GetRecentHitsParams{
		ArbType: string(CrossArbType),
		Limit:   5_000,
	})
}

func (r *Repository) GetRecentCrossArbs(ctx context.Context) ([]postgres.Arb, error) {
	return r.dbQuery.GetRecentArbs(ctx, postgres.GetRecentArbsParams{
		ArbType: string(CrossArbType),
		Limit:   5_000,
	})
}

func (r *Repository) GetSimilarityHitByCorrelationID(ctx context.Context, correlationId string) (postgres.SimilarityHit, error) {
	hit, err := r.dbQuery.GetSimilarityHitByCorrelationID(ctx, correlationId)
	if err != nil {
		return postgres.SimilarityHit{}, err
	}

	return hit, nil
}

func (r *Repository) GetArbByCorrelationID(ctx context.Context, correlationId string) (postgres.Arb, error) {
	hit, err := r.dbQuery.GetArbByCorrelationID(ctx, correlationId)
	if err != nil {
		return postgres.Arb{}, err
	}

	return hit, nil
}

func (r *Repository) BulkInsertSimilarityHits(ctx context.Context, bulk ...postgres.BulkInsertSimilarityHitsParams) (int64, error) {
	return r.dbQuery.BulkInsertSimilarityHits(ctx, bulk)
}

func (r *Repository) SoftDeleteSimilarityHit(ctx context.Context, correlationID string) error {
	return r.dbQuery.DeleteSimilarityHit(ctx, correlationID)
}

func (r *Repository) DeleteArb(ctx context.Context, correlationID string) error {
	return r.dbQuery.DeleteArb(ctx, correlationID)
}

func (r *Repository) UpdateArbConfirmedToTrue(ctx context.Context, correlationID string) error {
	return r.dbQuery.UpdateArbConfirm(ctx, postgres.UpdateArbConfirmParams{
		CorrelationID: correlationID,
		Confirmed:     true,
	})
}

func (r *Repository) UpdateArbRunning(ctx context.Context, correlationID string) error {
	return r.dbQuery.UpdateArbRunning(ctx, postgres.UpdateArbRunningParams{
		CorrelationID: correlationID,
		Running:       true,
	})
}

func (r *Repository) UpdateArbStatus(ctx context.Context, correlationID string) (postgres.Arb, error) {
	return r.dbQuery.UpdateArbStatus(ctx, postgres.UpdateArbStatusParams{
		CorrelationID: correlationID,
		Confirmed:     true,
		Running:       true,
	})
}

func (r *Repository) HardDeleteHit(ctx context.Context, deletedAt time.Time) error {
	return r.dbQuery.HardDeleteOldSimilarityHits(ctx, pgtype.Timestamptz{
		Time:  deletedAt,
		Valid: true,
	})
}

func (r *Repository) GetRunningCrossArbs(ctx context.Context) ([]postgres.Arb, error) {
	return r.dbQuery.GetRunninngArbs(ctx, string(CrossArbType))
}

func (r *Repository) SoftDeleteSimilarityHitsBulk(ctx context.Context, correlationIDs []string) error {
	return r.dbQuery.DeleteSimilarityHitsBulk(ctx, correlationIDs)
}

func (r *Repository) DeleteArbsBulk(ctx context.Context, correlationIDs []string) error {
	return r.dbQuery.DeleteArbsBulk(ctx, correlationIDs)
}

func (r *Repository) InsertNewOrder(ctx context.Context, param postgres.InsertOrderParams) error {
	return r.dbQuery.InsertOrder(ctx, param)
}

func (r *Repository) GetOrderByCorrelationID(ctx context.Context, correlationID string) (postgres.Order, error) {
	return r.dbQuery.GetOrderByCorrelationID(ctx, correlationID)
}

func (r *Repository) GetAllOrders(ctx context.Context) ([]postgres.Order, error) {
	return r.dbQuery.GetAllOrders(ctx)
}

func (r *Repository) InsertNewExcessFill(ctx context.Context, param postgres.InsertExcessFillParams) error {
	return r.dbQuery.InsertExcessFill(ctx, param)
}

func (r *Repository) GetExcessFillByCorrelationID(ctx context.Context, correlationID string) (postgres.ExcessFill, error) {
	return r.dbQuery.GetExcessFillByCorrelationID(ctx, correlationID)
}

func (r *Repository) GetAllExcessFills(ctx context.Context) ([]postgres.ExcessFill, error) {
	return r.dbQuery.GetAllExcessFills(ctx)
}

func (r *Repository) GetOrderWithExcess(ctx context.Context, limit uint32) ([]postgres.GetOrderWithExcessRow, error) {
	return r.dbQuery.GetOrderWithExcess(ctx, int32(limit))
}

