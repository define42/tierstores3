package meta

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"tierstore/internal/model"
)

func TestStoreSnapshotRestore(t *testing.T) {
	raftAddr := freeTCPAddr(t)
	dir := t.TempDir()

	store := newTestStore(t, Config{
		GatewayID:         "gw1",
		AdvertiseURL:      "http://127.0.0.1:19000",
		RaftAddr:          raftAddr,
		RaftDir:           dir,
		Bootstrap:         true,
		PlacementGroups:   32,
		ReplicationFactor: 2,
	})
	waitForLeader(t, store)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := store.EnsureBucket(ctx, "photos"); err != nil {
		t.Fatalf("ensure bucket: %v", err)
	}
	node, err := store.UpsertNode(ctx, &model.Node{
		ID:         "node1",
		URL:        "http://127.0.0.1:9101",
		WeightHot:  1,
		WeightWarm: 1,
		State:      model.NodeStateActive,
	})
	if err != nil {
		t.Fatalf("upsert node: %v", err)
	}
	now := time.Now().UTC().Round(0)
	obj := &model.Object{
		Bucket:         "photos",
		Key:            "trip/pic.jpg",
		ObjectID:       "object-1",
		Size:           7,
		ETag:           "\"etag\"",
		SHA256:         "abc123",
		PlacementGroup: 7,
		CurrentTier:    model.TierHot,
		HotReplicas:    []model.Replica{{NodeID: node.ID, URL: node.URL, Tier: model.TierHot, AddedAt: now}},
		CreatedAt:      now,
		UpdatedAt:      now,
		LastAccessedAt: now,
	}
	if _, err := store.PutObject(ctx, obj); err != nil {
		t.Fatalf("put object: %v", err)
	}
	if err := store.MarkAccessBatch(ctx, []model.AccessEvent{{
		Bucket: obj.Bucket,
		Key:    obj.Key,
		At:     now.Add(2 * time.Minute),
	}}); err != nil {
		t.Fatalf("mark access: %v", err)
	}
	if _, err := store.PatchObject(ctx, &ObjectPatch{
		Bucket:              obj.Bucket,
		Key:                 obj.Key,
		ExpectedObjectID:    obj.ObjectID,
		HasCurrentTier:      true,
		CurrentTier:         model.TierWarm,
		ReplaceWarmReplicas: true,
		WarmReplicas:        []model.Replica{{NodeID: node.ID, URL: node.URL, Tier: model.TierWarm, AddedAt: now}},
		ReplaceHotReplicas:  true,
	}); err != nil {
		t.Fatalf("patch object: %v", err)
	}
	snap := store.raft.Snapshot()
	if err := snap.Error(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened := newTestStore(t, Config{
		GatewayID:         "gw1",
		AdvertiseURL:      "http://127.0.0.1:19000",
		RaftAddr:          raftAddr,
		RaftDir:           dir,
		Bootstrap:         true,
		PlacementGroups:   32,
		ReplicationFactor: 2,
	})
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Fatalf("close reopened store: %v", err)
		}
	}()
	waitForLeader(t, reopened)

	st := reopened.Snapshot()
	if _, ok := st.Buckets["photos"]; !ok {
		t.Fatalf("bucket not restored")
	}
	got, err := reopened.GetObject("photos", "trip/pic.jpg")
	if err != nil {
		t.Fatalf("get restored object: %v", err)
	}
	if got.CurrentTier != model.TierWarm {
		t.Fatalf("unexpected tier after restore: %s", got.CurrentTier)
	}
	if len(got.WarmReplicas) != 1 || len(got.HotReplicas) != 0 {
		t.Fatalf("unexpected replicas after restore: hot=%d warm=%d", len(got.HotReplicas), len(got.WarmReplicas))
	}
	if got.LastAccessedAt.Before(now.Add(2 * time.Minute)) {
		t.Fatalf("last accessed not restored: %s", got.LastAccessedAt)
	}
	if st.Cluster.Epoch < 2 {
		t.Fatalf("cluster epoch not restored: %d", st.Cluster.Epoch)
	}
}

func newTestStore(t *testing.T, cfg Config) *Store {
	t.Helper()
	store, err := NewStore(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	return store
}

func waitForLeader(t *testing.T, store *Store) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if store.IsLeader() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("store did not become leader; raft dir=%s", filepath.Clean(store.cfg.RaftDir))
}
