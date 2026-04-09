package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"
	"web/protos"
	"web/service/repository/postgres"
	frontend "web/view/pages"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

type GrpcServer struct {
	protos.UnimplementedEsuOdaraServer
	app *App // Access to DB and Channels
}

func NewGrpcServer(app *App) *GrpcServer {
	return &GrpcServer{
		app: app,
	}
}

func (s *GrpcServer) Esu(stream protos.EsuOdara_EsuServer) error {
	ctx := stream.Context()

	if err := stream.SendHeader(nil); err != nil {
		return err
	}

	readErrCh := make(chan error, 1)
	sendCh := make(chan *protos.ServerEbo, 100)

	go func() {
		defer close(readErrCh)

		for {
			msg, err := stream.Recv() // receives from client
			if err == io.EOF {
				return
			}

			if err != nil {
				readErrCh <- err
				return
			}

			reply, err := s.app.ProcessEbo(ctx, msg)
			if err != nil {
				log.Printf("error processing Ebo from grpc client: %s", err.Error())
				continue
			}

			select {
			case sendCh <- reply:
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case err := <-readErrCh:
			return err

		case cmd := <-s.app.GrpcComms: // receives from app
			if cmd != nil {
				if err := stream.Send(cmd); err != nil {
					return status.Errorf(codes.Unavailable, "failed to send app command: %v", err)
				}
			}

		case msg := <-sendCh: // response from app to client
			if msg != nil {
				if err := stream.Send(msg); err != nil {
					return status.Errorf(codes.Unavailable, "failed to send reply: %v", err)
				}
			}
		}
	}
}

func (a *App) ProcessEbo(ctx context.Context, ebo *protos.ClientEbo) (*protos.ServerEbo, error) {

	switch p := ebo.Action.(type) {

	case *protos.ClientEbo_CrossPlatformHit:
		hit := p.CrossPlatformHit

		if hit == nil {
			return nil, errors.New("got nil from client")
		}

		byteData, err := protojson.Marshal(hit)
		if err != nil {
			return nil, err
		}

		return nil, a.db.InsertNewCrossHit(
			ctx,
			ebo.CorrelationId,
			time.UnixMilli(ebo.ActionAt).UTC(),
			byteData,
		)

	case *protos.ClientEbo_IntraPlatformHit:
		hit := p.IntraPlatformHit

		if hit == nil {
			return nil, errors.New("got nil from client")
		}

		byteData, err := frontend.ProtoMarshaler.Marshal(hit)
		if err != nil {
			return nil, err
		}

		return nil, a.db.InsertNewIntraHit(
			ctx,
			ebo.CorrelationId,
			time.UnixMilli(ebo.ActionAt).UTC(),
			byteData,
		)

	case *protos.ClientEbo_CrossPlatformArb:
		arb := p.CrossPlatformArb

		if arb == nil {
			return nil, errors.New("got nil from client")
		}

		byteData, err := protojson.Marshal(arb)
		if err != nil {
			return nil, err
		}

		return nil, a.db.InsertNewCrossArb(
			ctx,
			ebo.CorrelationId,
			time.UnixMilli(ebo.ActionAt).UTC(),
			byteData,
		)

	case *protos.ClientEbo_IntraPlatformArb:
		arb := p.IntraPlatformArb

		if arb == nil {
			return nil, errors.New("got nil from client")
		}

		byteData, err := protojson.Marshal(arb)
		if err != nil {
			return nil, err
		}

		return nil, a.db.InsertNewIntraArb(
			ctx,
			ebo.CorrelationId,
			time.UnixMilli(ebo.ActionAt).UTC(),
			byteData,
		)

	case *protos.ClientEbo_GetRunningArbs:
		req := p.GetRunningArbs

		if req == nil {
			return nil, errors.New("got nil from client")
		}

		switch req.ArbType {
		case protos.ArbType_CROSS_PLATFORM:
			runningArbs, err := a.db.GetRunningCrossArbs(ctx)
			if err != nil {
				return nil, fmt.Errorf("got error from db: %w", err)
			}

			arbs := make([]*protos.Arb, 0, len(runningArbs))
			correlationIds := make([]string, 0, len(runningArbs))

			for _, arb := range runningArbs {
				var arb_ebo protos.Arb
				if err := frontend.ProtoUnMarshaler.Unmarshal(arb.Arbs, &arb_ebo); err != nil {
					log.Println("error Unmarshaling arb:", err.Error())
					continue
				}

				arbs = append(arbs, &arb_ebo)
				correlationIds = append(correlationIds, arb.CorrelationID)
			}

			return &protos.ServerEbo{
				CorrelationId: "",
				ActionAt:      0,
				Action: &protos.ServerEbo_RunningArbsResponse{
					RunningArbsResponse: &protos.Arbs{
						CorrelationIds:  correlationIds,
						ConfirmedAndRun: arbs,
					},
				},
			}, nil

		case protos.ArbType_INTRA_PLATFORM:
			return nil, errors.New("protos.ArbType_INTRA_PLATFORM not implemented")

		default:
			return nil, errors.New("arbType not supported")
		}

	case *protos.ClientEbo_DeleteRunningArbs:
		req := p.DeleteRunningArbs
		if req == nil {
			return nil, errors.New("got nil from client")
		}

		for _, correlationID := range req.CorrelationIds {
			if err := a.db.DeleteArb(ctx, correlationID); err != nil {
				log.Printf("error deleting arb %s\n", err.Error())
			}
		}

	case *protos.ClientEbo_OrderSubmitted:
		req := p.OrderSubmitted
		if req == nil {
			return nil, errors.New("got nil from client")
		}

		return nil, a.db.InsertNewOrder(ctx, postgres.InsertOrderParams{
			OrderCorrelationID: ebo.CorrelationId,
			FoundAt: pgtype.Timestamptz{
				Time:  time.UnixMilli(ebo.ActionAt).UTC(),
				Valid: true,
			},
			ArbCorrelationID: req.ArbCorrelationId,
			AnchorCost:       decimal.NewFromFloat32(req.AnchorCost),
			MatchCost:        decimal.NewFromFloat32(req.MatchCost),
			AnchorFill:       decimal.NewFromFloat32(req.AnchorFill),
			MatchFill:        decimal.NewFromFloat32(req.MatchFill),
			ExcessFill:       decimal.NewFromFloat32(req.ExcessFill),
			AnchorOrderID:    req.AnchorOrderId,
			MatchOrderID:     req.MatchOrderId,
		})

	case *protos.ClientEbo_ExcessFillSubmitted:
		req := p.ExcessFillSubmitted
		if req == nil {
			return nil, errors.New("got nil from client")
		}

		return nil, a.db.InsertNewExcessFill(ctx, postgres.InsertExcessFillParams{
			CorrelationID: ebo.CorrelationId,
			FoundAt: pgtype.Timestamptz{
				Time:  time.UnixMilli(ebo.ActionAt).UTC(),
				Valid: true,
			},
			Platform: req.Platform.String(),
			OrderID:  req.OrderId,
			FillSize: decimal.NewFromFloat32(req.ExcessFillSize),
			FillCost: decimal.NewFromFloat32(req.ExcessFillCost),
		})

	case nil:
		return nil, fmt.Errorf("received Ebo with empty payload")
	}

	return nil, fmt.Errorf("received unknown Action: %+v", ebo)
}
