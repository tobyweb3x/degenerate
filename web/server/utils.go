package backend

import (
	"fmt"
	"slices"
	"time"
	"web/protos"
	frontend "web/view/pages"
)

func buildDiscoveredArbs(similarityHit *protos.SimilarityHit, mappings []LegMapping) ([]*protos.DiscoveredArb, error) {
	discoveredArbList := make([]*protos.DiscoveredArb, 0, len(mappings))

	for _, mapping := range mappings {
		index := slices.IndexFunc(similarityHit.Matches, func(candidate *protos.MatchCandidate) bool {
			if candidate == nil || candidate.MarketInfo == nil {
				return false
			}

			return candidate.MarketInfo.Uuid == mapping.MatchUUID
		})

		if index < 0 {
			continue
		}

		candidate := similarityHit.Matches[index]

		anchor := &protos.Discovery{
			MarketInfo: similarityHit.Anchor,
		}

		match := &protos.Discovery{
			MarketInfo: candidate.MarketInfo,
		}

		if anchor.LegStr = frontend.GetOutcomeByIndex(frontend.ParseOutcomes(similarityHit.Anchor.Outcome), mapping.AnchorLeg); anchor.LegStr == "?" {
			return nil, fmt.Errorf("anchor.LegStr is ?")
		}

		if match.LegStr = frontend.GetOutcomeByIndex(frontend.ParseOutcomes(candidate.MarketInfo.Outcome), mapping.MatchLeg); match.LegStr == "?" {
			return nil, fmt.Errorf("match.LegStr is ?")
		}

		var err error
		if anchor.Leg, err = setLeg(mapping.AnchorLeg); err != nil {
			return nil, err
		}

		if match.Leg, err = setLeg(mapping.MatchLeg); err != nil {
			return nil, err
		}

		discoveredArbList = append(discoveredArbList, &protos.DiscoveredArb{
			Anchor: anchor,
			Match:  match,
			Scored: candidate.Scored,
		})
	}

	return discoveredArbList, nil
}

func setLeg(leg int) (protos.Leg, error) {
	switch leg {
	case 0:
		return protos.Leg_LEG_LEFT, nil
	case 1:
		return protos.Leg_LEG_RIGHT, nil
	}

	return 0, fmt.Errorf("wrong leg: %d", leg)
}

func newCrossPlatformArbDiscovery(arbs []*protos.DiscoveredArb) *protos.Ebo {
	return &protos.Ebo{
		FoundAt: time.Now().UnixMilli(),
		Action: &protos.Ebo_CrossPlatformArbDiscovery{
			CrossPlatformArbDiscovery: &protos.DiscoveredArbList{
				Arbs: arbs,
			},
		},
	}
}
