package frontend

import (
	"encoding/json"
	"time"
	"web/protos"
	"web/service/repository/postgres"
)

// TemplateModel is a generic container for any Ebo payload type
type TemplateModel[T any] struct {
	CorrelationId string
	Timestamp     time.Time
	Payload       T
}

func FromCrossPlatformArbs(
	param ...postgres.NeedsResolve,
) ([]TemplateModel[protos.Ebo_CrossPlatformArb], error) {

	r := make([]TemplateModel[protos.Ebo_CrossPlatformArb], 0, len(param))

	for _, v := range param {
		var hit protos.SimilarityHit

		if len(v.SimilarityHit) == 0 {
			continue
		}

		if err := json.Unmarshal(v.SimilarityHit, &hit); err != nil {
			return nil, err
		}

		r = append(r, TemplateModel[protos.Ebo_CrossPlatformArb]{
			CorrelationId: v.CorrelationID,
			Timestamp:     v.Timestamp.Time,
			Payload:       protos.Ebo_CrossPlatformArb{CrossPlatformArb: &hit},
		})
	}

	return r, nil
}
