package backend

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"
	"web/protos"
	frontend "web/view/pages"

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
	sendCh := make(chan *protos.Ebo, 100)

	go func() {
		defer close(readErrCh)

		for {
			msg, err := stream.Recv()
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

		case cmd := <-s.app.GrpcComms:
			if cmd != nil {
				if err := stream.Send(cmd); err != nil {
					return status.Errorf(codes.Unavailable, "failed to send app command: %v", err)
				}
			}

		case msg := <-sendCh:
			if msg != nil {
				if err := stream.Send(msg); err != nil {
					return status.Errorf(codes.Unavailable, "failed to send reply: %v", err)
				}
			}
		}
	}
}

func (a *App) ProcessEbo(ctx context.Context, ebo *protos.Ebo) (*protos.Ebo, error) {

	switch p := ebo.Action.(type) {

	case *protos.Ebo_CrossPlatformHit:
		hit := p.CrossPlatformHit

		byteData, err := protojson.Marshal(hit)
		if err != nil {
			return nil, err
		}

		return nil, a.db.InsertNewCrossHit(
			ctx,
			ebo.CorrelationId,
			time.UnixMilli(ebo.FoundAt).UTC(),
			byteData,
		)

	case *protos.Ebo_IntraPlatformHit:
		hit := p.IntraPlatformHit
		byteData, err := frontend.ProtoMarshaler.Marshal(hit)
		if err != nil {
			return nil, err
		}

		return nil, a.db.InsertNewIntraHit(
			ctx,
			ebo.CorrelationId,
			time.UnixMilli(ebo.FoundAt).UTC(),
			byteData,
		)

	case *protos.Ebo_CrossPlatformArb:
		arb := p.CrossPlatformArb

		byteData, err := protojson.Marshal(arb)
		if err != nil {
			return nil, err
		}

		return nil, a.db.InsertNewCrossArb(
			ctx,
			ebo.CorrelationId,
			time.UnixMilli(ebo.FoundAt).UTC(),
			byteData,
		)

	case *protos.Ebo_IntraPlatformArb:
		arb := p.IntraPlatformArb

		byteData, err := protojson.Marshal(arb)
		if err != nil {
			return nil, err
		}

		return nil, a.db.InsertNewIntraArb(
			ctx,
			ebo.CorrelationId,
			time.UnixMilli(ebo.FoundAt).UTC(),
			byteData,
		)

	case nil:
		return nil, fmt.Errorf("received Ebo with empty payload")

	}

	return nil, fmt.Errorf("received unknown Action")
}
