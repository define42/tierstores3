package model

import (
	"fmt"
	"sort"
	"time"
)

type Tier string

const (
	TierHot  Tier = "hot"
	TierWarm Tier = "warm"
)

type NodeState string

const (
	NodeStateActive   NodeState = "active"
	NodeStateDraining NodeState = "draining"
	NodeStateRemoved  NodeState = "removed"
)

type Node struct {
	ID         string    `json:"id"`
	URL        string    `json:"url"`
	WeightHot  float64   `json:"weight_hot"`
	WeightWarm float64   `json:"weight_warm"`
	Zone       string    `json:"zone,omitempty"`
	State      NodeState `json:"state"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type Replica struct {
	NodeID  string    `json:"node_id"`
	URL     string    `json:"url"`
	Tier    Tier      `json:"tier"`
	AddedAt time.Time `json:"added_at"`
}

type Bucket struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

type Object struct {
	Bucket         string    `json:"bucket"`
	Key            string    `json:"key"`
	ObjectID       string    `json:"object_id"`
	Size           int64     `json:"size"`
	ETag           string    `json:"etag"`
	SHA256         string    `json:"sha256"`
	PlacementGroup uint64    `json:"placement_group"`
	CurrentTier    Tier      `json:"current_tier"`
	HotReplicas    []Replica `json:"hot_replicas,omitempty"`
	WarmReplicas   []Replica `json:"warm_replicas,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	LastAccessedAt time.Time `json:"last_accessed_at"`
	Deleted        bool      `json:"deleted"`
}

type Cluster struct {
	Epoch             uint64           `json:"epoch"`
	PlacementGroups   uint64           `json:"placement_groups"`
	ReplicationFactor int              `json:"replication_factor"`
	Nodes             map[string]*Node `json:"nodes"`
}

type State struct {
	Buckets map[string]*Bucket `json:"buckets"`
	Objects map[string]*Object `json:"objects"`
	Cluster Cluster            `json:"cluster"`
}

func ObjectMapKey(bucket, key string) string {
	return fmt.Sprintf("%s\x00%s", bucket, key)
}

func (o *Object) ReplicasForTier(t Tier) []Replica {
	switch t {
	case TierHot:
		return cloneReplicas(o.HotReplicas)
	case TierWarm:
		return cloneReplicas(o.WarmReplicas)
	default:
		return nil
	}
}

func (o *Object) SetReplicasForTier(t Tier, replicas []Replica) {
	switch t {
	case TierHot:
		o.HotReplicas = cloneReplicas(replicas)
	case TierWarm:
		o.WarmReplicas = cloneReplicas(replicas)
	}
}

func CloneNode(n *Node) *Node {
	if n == nil {
		return nil
	}
	cp := *n
	return &cp
}

func CloneObject(o *Object) *Object {
	if o == nil {
		return nil
	}
	cp := *o
	cp.HotReplicas = cloneReplicas(o.HotReplicas)
	cp.WarmReplicas = cloneReplicas(o.WarmReplicas)
	return &cp
}

func CloneState(s State) State {
	out := State{
		Buckets: make(map[string]*Bucket, len(s.Buckets)),
		Objects: make(map[string]*Object, len(s.Objects)),
		Cluster: Cluster{
			Epoch:             s.Cluster.Epoch,
			PlacementGroups:   s.Cluster.PlacementGroups,
			ReplicationFactor: s.Cluster.ReplicationFactor,
			Nodes:             make(map[string]*Node, len(s.Cluster.Nodes)),
		},
	}
	for k, b := range s.Buckets {
		if b == nil {
			continue
		}
		cp := *b
		out.Buckets[k] = &cp
	}
	for k, o := range s.Objects {
		out.Objects[k] = CloneObject(o)
	}
	for k, n := range s.Cluster.Nodes {
		out.Cluster.Nodes[k] = CloneNode(n)
	}
	return out
}

func cloneReplicas(in []Replica) []Replica {
	if len(in) == 0 {
		return nil
	}
	out := make([]Replica, len(in))
	copy(out, in)
	return out
}

func SortedBucketNames(m map[string]*Bucket) []string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
