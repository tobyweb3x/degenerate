package frontend

import (
	"time"
	"web/protos"
	"web/service/repository/postgres"

	"google.golang.org/protobuf/encoding/protojson"
)

var ProtoMarshaler = protojson.MarshalOptions{
	UseProtoNames:   true,
	EmitUnpopulated: true,
}

var ProtoUnMarshaler = protojson.UnmarshalOptions{
	DiscardUnknown: true,
}

// TemplateModel is a generic container for any Ebo payload type
type TemplateModel[T any] struct {
	CorrelationId string
	ArbFoundAt    time.Time
	Payload       T
}

func ToCrossPlatformHits(
	param ...postgres.SimilarityHit,
) ([]TemplateModel[protos.Ebo_CrossPlatformHit], error) {

	r := make([]TemplateModel[protos.Ebo_CrossPlatformHit], 0, len(param))

	for _, v := range param {
		var hit protos.SimilarityHit

		if len(v.SimilarityHit) == 0 {
			continue
		}

		if err := ProtoUnMarshaler.Unmarshal(v.SimilarityHit, &hit); err != nil {
			return nil, err
		}

		r = append(r, TemplateModel[protos.Ebo_CrossPlatformHit]{
			CorrelationId: v.CorrelationID,
			ArbFoundAt:    v.FoundAt.Time,
			Payload:       protos.Ebo_CrossPlatformHit{CrossPlatformHit: &hit},
		})
	}

	return r, nil
}

func ToCrossPlatformArbs(
	param ...postgres.Arb,
) ([]TemplateModel[protos.Ebo_CrossPlatformArb], error) {

	r := make([]TemplateModel[protos.Ebo_CrossPlatformArb], 0, len(param))

	for _, v := range param {
		var arb protos.Arb

		if len(v.Arbs) == 0 {
			continue
		}

		if err := ProtoUnMarshaler.Unmarshal(v.Arbs, &arb); err != nil {
			return nil, err
		}

		r = append(r, TemplateModel[protos.Ebo_CrossPlatformArb]{
			CorrelationId: v.CorrelationID,
			ArbFoundAt:    v.FoundAt.Time,
			Payload:       protos.Ebo_CrossPlatformArb{CrossPlatformArb: &arb},
		})
	}

	return r, nil
}
