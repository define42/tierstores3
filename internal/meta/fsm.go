package meta

import (
	"encoding/json"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"

	"tierstore/internal/model"
)

type commandType string

const (
	commandEnsureBucket commandType = "ensure_bucket"
	commandPutObject    commandType = "put_object"
	commandDeleteObject commandType = "delete_object"
	commandMarkAccesses commandType = "mark_access_batch"
	commandUpsertNode   commandType = "upsert_node"
	commandSetNodeState commandType = "set_node_state"
	commandRemoveNode   commandType = "remove_node"
	commandPatchObject  commandType = "patch_object"
)

type command struct {
	Type        commandType         `json:"type"`
	At          time.Time           `json:"at,omitempty"`
	Bucket      string              `json:"bucket,omitempty"`
	Key         string              `json:"key,omitempty"`
	Object      *model.Object       `json:"object,omitempty"`
	Node        *model.Node         `json:"node,omitempty"`
	NodeID      string              `json:"node_id,omitempty"`
	NodeState   model.NodeState     `json:"node_state,omitempty"`
	Events      []model.AccessEvent `json:"events,omitempty"`
	ObjectPatch *ObjectPatch        `json:"object_patch,omitempty"`
}

type commandResult struct {
	Object *model.Object
	Node   *model.Node
	Err    string
}

type ObjectPatch struct {
	Bucket              string          `json:"bucket"`
	Key                 string          `json:"key"`
	ExpectedObjectID    string          `json:"expected_object_id,omitempty"`
	OnlyIfCurrentTier   bool            `json:"only_if_current_tier,omitempty"`
	RequiredCurrentTier model.Tier      `json:"required_current_tier,omitempty"`
	HasCurrentTier      bool            `json:"has_current_tier,omitempty"`
	CurrentTier         model.Tier      `json:"current_tier,omitempty"`
	ReplaceHotReplicas  bool            `json:"replace_hot_replicas,omitempty"`
	HotReplicas         []model.Replica `json:"hot_replicas,omitempty"`
	ReplaceWarmReplicas bool            `json:"replace_warm_replicas,omitempty"`
	WarmReplicas        []model.Replica `json:"warm_replicas,omitempty"`
	HasLastAccessedAt   bool            `json:"has_last_accessed_at,omitempty"`
	LastAccessedAt      time.Time       `json:"last_accessed_at,omitempty"`
}

type fsm struct {
	mu sync.RWMutex
	st model.State
}

func newFSM(placementGroups uint64, replicationFactor int) *fsm {
	if placementGroups == 0 {
		placementGroups = 1024
	}
	if replicationFactor == 0 {
		replicationFactor = 3
	}
	return &fsm{
		st: model.State{
			Buckets: make(map[string]*model.Bucket),
			Objects: make(map[string]*model.Object),
			Cluster: model.Cluster{
				Epoch:             1,
				PlacementGroups:   placementGroups,
				ReplicationFactor: replicationFactor,
				Nodes:             make(map[string]*model.Node),
			},
		},
	}
}

func (f *fsm) Apply(log *raft.Log) interface{} {
	var cmd command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return &commandResult{Err: err.Error()}
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Type {
	case commandEnsureBucket:
		return f.applyEnsureBucket(cmd.Bucket, cmd.At)
	case commandPutObject:
		return f.applyPutObject(cmd.Object)
	case commandDeleteObject:
		return f.applyDeleteObject(cmd.Bucket, cmd.Key)
	case commandMarkAccesses:
		return f.applyMarkAccessBatch(cmd.Events)
	case commandUpsertNode:
		return f.applyUpsertNode(cmd.Node, cmd.At)
	case commandSetNodeState:
		return f.applySetNodeState(cmd.NodeID, cmd.NodeState, cmd.At)
	case commandRemoveNode:
		return f.applyRemoveNode(cmd.NodeID, cmd.At)
	case commandPatchObject:
		return f.applyPatchObject(cmd.ObjectPatch)
	default:
		return &commandResult{Err: "unknown command type"}
	}
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{state: f.snapshot()}, nil
}

func (f *fsm) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()
	var st model.State
	if err := json.NewDecoder(snapshot).Decode(&st); err != nil {
		return err
	}
	if st.Buckets == nil {
		st.Buckets = make(map[string]*model.Bucket)
	}
	if st.Objects == nil {
		st.Objects = make(map[string]*model.Object)
	}
	if st.Cluster.Nodes == nil {
		st.Cluster.Nodes = make(map[string]*model.Node)
	}
	if st.Cluster.PlacementGroups == 0 {
		st.Cluster.PlacementGroups = 1024
	}
	if st.Cluster.ReplicationFactor == 0 {
		st.Cluster.ReplicationFactor = 3
	}
	if st.Cluster.Epoch == 0 {
		st.Cluster.Epoch = 1
	}
	f.mu.Lock()
	f.st = model.CloneState(st)
	f.mu.Unlock()
	return nil
}

func (f *fsm) snapshot() model.State {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return model.CloneState(f.st)
}

func (f *fsm) bucketExists(name string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, ok := f.st.Buckets[name]
	return ok
}

func (f *fsm) getObject(bucket, key string) (*model.Object, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	obj, ok := f.st.Objects[model.ObjectMapKey(bucket, key)]
	if !ok {
		return nil, ErrNotFound
	}
	return model.CloneObject(obj), nil
}

func (f *fsm) applyEnsureBucket(name string, at time.Time) *commandResult {
	if name == "" {
		return &commandResult{Err: "bucket name is required"}
	}
	if _, ok := f.st.Buckets[name]; ok {
		return &commandResult{}
	}
	f.st.Buckets[name] = &model.Bucket{Name: name, CreatedAt: at.UTC()}
	return &commandResult{}
}

func (f *fsm) applyPutObject(obj *model.Object) *commandResult {
	if obj == nil {
		return &commandResult{Err: "nil object"}
	}
	if _, ok := f.st.Buckets[obj.Bucket]; !ok {
		f.st.Buckets[obj.Bucket] = &model.Bucket{Name: obj.Bucket, CreatedAt: obj.CreatedAt.UTC()}
	}
	mapKey := model.ObjectMapKey(obj.Bucket, obj.Key)
	old := model.CloneObject(f.st.Objects[mapKey])
	f.st.Objects[mapKey] = model.CloneObject(obj)
	return &commandResult{Object: old}
}

func (f *fsm) applyDeleteObject(bucket, key string) *commandResult {
	mapKey := model.ObjectMapKey(bucket, key)
	obj, ok := f.st.Objects[mapKey]
	if !ok {
		return &commandResult{Err: ErrNotFound.Error()}
	}
	delete(f.st.Objects, mapKey)
	return &commandResult{Object: model.CloneObject(obj)}
}

func (f *fsm) applyMarkAccessBatch(events []model.AccessEvent) *commandResult {
	for _, ev := range events {
		obj, ok := f.st.Objects[model.ObjectMapKey(ev.Bucket, ev.Key)]
		if !ok {
			continue
		}
		if ev.At.After(obj.LastAccessedAt) {
			obj.LastAccessedAt = ev.At.UTC()
		}
	}
	return &commandResult{}
}

func (f *fsm) applyUpsertNode(node *model.Node, at time.Time) *commandResult {
	if node == nil {
		return &commandResult{Err: "nil node"}
	}
	cp := model.CloneNode(node)
	if existing, ok := f.st.Cluster.Nodes[node.ID]; ok {
		cp.CreatedAt = existing.CreatedAt
	} else {
		cp.CreatedAt = at.UTC()
	}
	cp.UpdatedAt = at.UTC()
	if cp.State == "" {
		cp.State = model.NodeStateActive
	}
	if cp.WeightHot <= 0 {
		cp.WeightHot = 1
	}
	if cp.WeightWarm <= 0 {
		cp.WeightWarm = 1
	}
	f.st.Cluster.Nodes[node.ID] = cp
	f.st.Cluster.Epoch++
	return &commandResult{Node: model.CloneNode(cp)}
}

func (f *fsm) applySetNodeState(id string, state model.NodeState, at time.Time) *commandResult {
	node, ok := f.st.Cluster.Nodes[id]
	if !ok {
		return &commandResult{Err: ErrNotFound.Error()}
	}
	node.State = state
	node.UpdatedAt = at.UTC()
	f.st.Cluster.Epoch++
	return &commandResult{Node: model.CloneNode(node)}
}

func (f *fsm) applyRemoveNode(id string, at time.Time) *commandResult {
	node, ok := f.st.Cluster.Nodes[id]
	if !ok {
		return &commandResult{Err: ErrNotFound.Error()}
	}
	node.State = model.NodeStateRemoved
	node.UpdatedAt = at.UTC()
	f.st.Cluster.Epoch++
	return &commandResult{Node: model.CloneNode(node)}
}

func (f *fsm) applyPatchObject(patch *ObjectPatch) *commandResult {
	if patch == nil {
		return &commandResult{Err: "nil object patch"}
	}
	obj, ok := f.st.Objects[model.ObjectMapKey(patch.Bucket, patch.Key)]
	if !ok {
		return &commandResult{Err: ErrNotFound.Error()}
	}
	if patch.ExpectedObjectID != "" && obj.ObjectID != patch.ExpectedObjectID {
		return &commandResult{Err: ErrConflict.Error()}
	}
	if patch.OnlyIfCurrentTier && obj.CurrentTier != patch.RequiredCurrentTier {
		return &commandResult{Object: model.CloneObject(obj)}
	}
	if patch.HasCurrentTier {
		obj.CurrentTier = patch.CurrentTier
	}
	if patch.ReplaceHotReplicas {
		obj.HotReplicas = cloneReplicas(patch.HotReplicas)
	}
	if patch.ReplaceWarmReplicas {
		obj.WarmReplicas = cloneReplicas(patch.WarmReplicas)
	}
	if patch.HasLastAccessedAt && patch.LastAccessedAt.After(obj.LastAccessedAt) {
		obj.LastAccessedAt = patch.LastAccessedAt.UTC()
	}
	return &commandResult{Object: model.CloneObject(obj)}
}

type fsmSnapshot struct {
	state model.State
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	enc := json.NewEncoder(sink)
	if err := enc.Encode(s.state); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

func cloneReplicas(in []model.Replica) []model.Replica {
	if len(in) == 0 {
		return nil
	}
	out := make([]model.Replica, len(in))
	copy(out, in)
	return out
}

func commandError(err string) error {
	switch err {
	case "":
		return nil
	case ErrNotFound.Error():
		return ErrNotFound
	case ErrConflict.Error():
		return ErrConflict
	default:
		return errors.New(err)
	}
}
