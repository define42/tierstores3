package placement

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"tierstore/internal/model"
)

type Selector struct{}

func NewSelector() *Selector { return &Selector{} }

type candidate struct {
	node  *model.Node
	score float64
}

func (s *Selector) PlacementGroup(bucket, key string, groups uint64) uint64 {
	if groups == 0 {
		groups = 1
	}
	return hash64(fmt.Sprintf("pg:%s\x00%s", bucket, key)) % groups
}

func (s *Selector) Select(bucket, key string, tier model.Tier, cluster model.Cluster, includeDraining bool) ([]*model.Node, uint64, error) {
	pg := s.PlacementGroup(bucket, key, cluster.PlacementGroups)
	seed := fmt.Sprintf("%s|%s|%s|%d", bucket, key, tier, pg)
	cands := make([]candidate, 0, len(cluster.Nodes))
	for _, node := range cluster.Nodes {
		if node == nil {
			continue
		}
		if node.State == model.NodeStateRemoved {
			continue
		}
		if !includeDraining && node.State == model.NodeStateDraining {
			continue
		}
		weight := node.WeightHot
		if tier == model.TierWarm {
			weight = node.WeightWarm
		}
		if weight <= 0 {
			continue
		}
		score := weightedScore(seed, node.ID, weight)
		cands = append(cands, candidate{node: model.CloneNode(node), score: score})
	}
	if len(cands) < cluster.ReplicationFactor {
		return nil, pg, fmt.Errorf("need %d nodes for %s placement, have %d", cluster.ReplicationFactor, tier, len(cands))
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].score == cands[j].score {
			return cands[i].node.ID < cands[j].node.ID
		}
		return cands[i].score > cands[j].score
	})
	out := make([]*model.Node, 0, cluster.ReplicationFactor)
	usedZones := make(map[string]struct{})
	for _, cand := range cands {
		if len(out) == cluster.ReplicationFactor {
			break
		}
		if cand.node.Zone != "" {
			if _, ok := usedZones[cand.node.Zone]; ok {
				continue
			}
			usedZones[cand.node.Zone] = struct{}{}
		}
		out = append(out, cand.node)
	}
	for _, cand := range cands {
		if len(out) == cluster.ReplicationFactor {
			break
		}
		already := false
		for _, existing := range out {
			if existing.ID == cand.node.ID {
				already = true
				break
			}
		}
		if already {
			continue
		}
		out = append(out, cand.node)
	}
	if len(out) < cluster.ReplicationFactor {
		return nil, pg, fmt.Errorf("insufficient distinct nodes after zone spread: need %d, got %d", cluster.ReplicationFactor, len(out))
	}
	return out, pg, nil
}

func weightedScore(seed, nodeID string, weight float64) float64 {
	if weight <= 0 {
		return math.Inf(-1)
	}
	h := hash64(seed + "|" + nodeID)
	u := float64(h>>1+1) / float64(math.MaxUint64)
	if u <= 0 {
		u = math.SmallestNonzeroFloat64
	}
	return weight / -math.Log(u)
}

func hash64(s string) uint64 {
	digest := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(digest[:8])
}
