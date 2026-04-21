package meta

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"tierstore/internal/model"
)

var ErrNotFound = errors.New("not found")

type Store struct {
	mu   sync.RWMutex
	path string
	st   model.State
}

func NewStore(path string, placementGroups uint64, replicationFactor int) (*Store, error) {
	s := &Store{
		path: path,
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
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) load() error {
	if s.path == "" {
		return nil
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read state: %w", err)
	}
	var st model.State
	if err := json.Unmarshal(data, &st); err != nil {
		return fmt.Errorf("decode state: %w", err)
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
	s.st = st
	return nil
}

func (s *Store) saveLocked() error {
	if s.path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("mkdir state dir: %w", err)
	}
	data, err := json.MarshalIndent(s.st, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write temp state: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("rename state: %w", err)
	}
	return nil
}

func (s *Store) Snapshot() model.State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return model.CloneState(s.st)
}

func (s *Store) EnsureBucket(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.st.Buckets[name]; ok {
		return nil
	}
	s.st.Buckets[name] = &model.Bucket{Name: name, CreatedAt: time.Now().UTC()}
	return s.saveLocked()
}

func (s *Store) BucketExists(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.st.Buckets[name]
	return ok
}

func (s *Store) PutObject(obj *model.Object) error {
	if obj == nil {
		return errors.New("nil object")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.st.Buckets[obj.Bucket]; !ok {
		s.st.Buckets[obj.Bucket] = &model.Bucket{Name: obj.Bucket, CreatedAt: time.Now().UTC()}
	}
	s.st.Objects[model.ObjectMapKey(obj.Bucket, obj.Key)] = model.CloneObject(obj)
	return s.saveLocked()
}

func (s *Store) GetObject(bucket, key string) (*model.Object, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	obj, ok := s.st.Objects[model.ObjectMapKey(bucket, key)]
	if !ok {
		return nil, ErrNotFound
	}
	return model.CloneObject(obj), nil
}

func (s *Store) UpdateObject(bucket, key string, fn func(*model.Object) error) (*model.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.st.Objects[model.ObjectMapKey(bucket, key)]
	if !ok {
		return nil, ErrNotFound
	}
	cp := model.CloneObject(obj)
	if err := fn(cp); err != nil {
		return nil, err
	}
	s.st.Objects[model.ObjectMapKey(bucket, key)] = cp
	if err := s.saveLocked(); err != nil {
		return nil, err
	}
	return model.CloneObject(cp), nil
}

func (s *Store) DeleteObject(bucket, key string) (*model.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	mapKey := model.ObjectMapKey(bucket, key)
	obj, ok := s.st.Objects[mapKey]
	if !ok {
		return nil, ErrNotFound
	}
	delete(s.st.Objects, mapKey)
	if err := s.saveLocked(); err != nil {
		return nil, err
	}
	return model.CloneObject(obj), nil
}

func (s *Store) MarkAccess(bucket, key string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.st.Objects[model.ObjectMapKey(bucket, key)]
	if !ok {
		return ErrNotFound
	}
	if at.After(obj.LastAccessedAt) {
		obj.LastAccessedAt = at.UTC()
		obj.UpdatedAt = time.Now().UTC()
		return s.saveLocked()
	}
	return nil
}

func (s *Store) UpsertNode(node *model.Node) error {
	if node == nil {
		return errors.New("nil node")
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := model.CloneNode(node)
	if existing, ok := s.st.Cluster.Nodes[node.ID]; ok {
		cp.CreatedAt = existing.CreatedAt
		if cp.CreatedAt.IsZero() {
			cp.CreatedAt = now
		}
	} else {
		cp.CreatedAt = now
	}
	cp.UpdatedAt = now
	if cp.State == "" {
		cp.State = model.NodeStateActive
	}
	if cp.WeightHot <= 0 {
		cp.WeightHot = 1
	}
	if cp.WeightWarm <= 0 {
		cp.WeightWarm = 1
	}
	s.st.Cluster.Nodes[node.ID] = cp
	s.st.Cluster.Epoch++
	return s.saveLocked()
}

func (s *Store) SetNodeState(id string, state model.NodeState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	node, ok := s.st.Cluster.Nodes[id]
	if !ok {
		return ErrNotFound
	}
	node.State = state
	node.UpdatedAt = time.Now().UTC()
	s.st.Cluster.Epoch++
	return s.saveLocked()
}

func (s *Store) RemoveNode(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	node, ok := s.st.Cluster.Nodes[id]
	if !ok {
		return ErrNotFound
	}
	node.State = model.NodeStateRemoved
	node.UpdatedAt = time.Now().UTC()
	s.st.Cluster.Epoch++
	return s.saveLocked()
}
