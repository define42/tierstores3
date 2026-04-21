package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"tierstore/internal/meta"
	"tierstore/internal/model"
)

func (s *Server) runAccessTracker(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.AccessFlushInterval)
	defer ticker.Stop()
	pending := make(map[string]model.AccessEvent)
	flush := func(flushCtx context.Context) {
		if len(pending) == 0 {
			return
		}
		events := make([]model.AccessEvent, 0, len(pending))
		for _, ev := range pending {
			events = append(events, ev)
		}
		if err := s.store.MarkAccessBatch(flushCtx, events); err != nil {
			return
		}
		for k := range pending {
			delete(pending, k)
		}
	}
	for {
		select {
		case <-ctx.Done():
			flush(ctx)
			return
		case ev := <-s.accessCh:
			k := model.ObjectMapKey(ev.Bucket, ev.Key)
			if current, ok := pending[k]; !ok || ev.At.After(current.At) {
				pending[k] = ev
			}
		case <-ticker.C:
			flush(ctx)
		}
	}
}

func (s *Server) runLeaderLoops(ctx context.Context) {
	var cancel context.CancelFunc
	start := func() {
		if cancel != nil {
			return
		}
		leaderCtx, leaderCancel := context.WithCancel(ctx)
		cancel = leaderCancel
		go s.runTieringLoop(leaderCtx)
		go s.runRebalanceLoop(leaderCtx)
	}
	stop := func() {
		if cancel == nil {
			return
		}
		cancel()
		cancel = nil
	}
	if s.store.IsLeader() {
		start()
	}
	for {
		select {
		case <-ctx.Done():
			stop()
			return
		case isLeader := <-s.store.LeaderCh():
			if isLeader {
				start()
			} else {
				stop()
			}
		}
	}
}

func (s *Server) runTieringLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.TierScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			st := s.store.Snapshot()
			now := time.Now().UTC()
			for _, obj := range st.Objects {
				if obj == nil || obj.CurrentTier != model.TierHot {
					continue
				}
				if now.Sub(obj.LastAccessedAt) >= s.cfg.ColdAfter {
					_ = s.demoteObject(ctx, obj.Bucket, obj.Key)
				}
			}
		}
	}
}

func (s *Server) runRebalanceLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.RebalanceInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			st := s.store.Snapshot()
			for _, obj := range st.Objects {
				if obj == nil {
					continue
				}
				_ = s.ensurePlacement(ctx, obj.Bucket, obj.Key)
			}
		}
	}
}

func (s *Server) demoteObject(ctx context.Context, bucket, key string) error {
	unlock := s.locks.Lock(model.ObjectMapKey(bucket, key))
	defer unlock()

	obj, err := s.store.GetObject(bucket, key)
	if err != nil {
		return err
	}
	if obj.CurrentTier != model.TierHot {
		return nil
	}
	if time.Since(obj.LastAccessedAt) < s.cfg.ColdAfter {
		return nil
	}
	cluster := s.store.Snapshot().Cluster
	targetNodes, _, err := s.selector.Select(bucket, key, model.TierWarm, cluster, false)
	if err != nil {
		return err
	}
	targetReplicas := replicasFromNodes(targetNodes, model.TierWarm)
	tmp, cleanup, err := s.downloadObjectToTemp(ctx, sourceReplicas(obj, model.TierHot), obj.ObjectID, obj.SHA256)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := s.uploadReplicas(ctx, tmp.path, tmp.size, obj.SHA256, obj.ObjectID, targetReplicas); err != nil {
		return err
	}
	_, err = s.store.PatchObject(ctx, &meta.ObjectPatch{
		Bucket:              bucket,
		Key:                 key,
		ExpectedObjectID:    obj.ObjectID,
		HasCurrentTier:      true,
		CurrentTier:         model.TierWarm,
		ReplaceWarmReplicas: true,
		WarmReplicas:        targetReplicas,
	})
	if err != nil {
		return err
	}
	s.bestEffortDeleteReplicas(ctx, obj.HotReplicas, obj.ObjectID)
	_, _ = s.store.PatchObject(ctx, &meta.ObjectPatch{
		Bucket:              bucket,
		Key:                 key,
		ExpectedObjectID:    obj.ObjectID,
		OnlyIfCurrentTier:   true,
		RequiredCurrentTier: model.TierWarm,
		ReplaceHotReplicas:  true,
	})
	return nil
}

func (s *Server) promoteObject(ctx context.Context, bucket, key string) error {
	unlock := s.locks.Lock(model.ObjectMapKey(bucket, key))
	defer unlock()

	obj, err := s.store.GetObject(bucket, key)
	if err != nil {
		return err
	}
	if obj.CurrentTier != model.TierWarm {
		return nil
	}
	cluster := s.store.Snapshot().Cluster
	targetNodes, _, err := s.selector.Select(bucket, key, model.TierHot, cluster, false)
	if err != nil {
		return err
	}
	targetReplicas := replicasFromNodes(targetNodes, model.TierHot)
	tmp, cleanup, err := s.downloadObjectToTemp(ctx, sourceReplicas(obj, model.TierWarm), obj.ObjectID, obj.SHA256)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := s.uploadReplicas(ctx, tmp.path, tmp.size, obj.SHA256, obj.ObjectID, targetReplicas); err != nil {
		return err
	}
	accessedAt := time.Now().UTC()
	_, err = s.store.PatchObject(ctx, &meta.ObjectPatch{
		Bucket:             bucket,
		Key:                key,
		ExpectedObjectID:   obj.ObjectID,
		HasCurrentTier:     true,
		CurrentTier:        model.TierHot,
		ReplaceHotReplicas: true,
		HotReplicas:        targetReplicas,
		HasLastAccessedAt:  true,
		LastAccessedAt:     accessedAt,
	})
	if err != nil {
		return err
	}
	s.bestEffortDeleteReplicas(ctx, obj.WarmReplicas, obj.ObjectID)
	_, _ = s.store.PatchObject(ctx, &meta.ObjectPatch{
		Bucket:              bucket,
		Key:                 key,
		ExpectedObjectID:    obj.ObjectID,
		OnlyIfCurrentTier:   true,
		RequiredCurrentTier: model.TierHot,
		ReplaceWarmReplicas: true,
	})
	return nil
}

func (s *Server) ensurePlacement(ctx context.Context, bucket, key string) error {
	unlock := s.locks.Lock(model.ObjectMapKey(bucket, key))
	defer unlock()

	obj, err := s.store.GetObject(bucket, key)
	if err != nil {
		if err == meta.ErrNotFound {
			return nil
		}
		return err
	}
	cluster := s.store.Snapshot().Cluster
	desiredNodes, _, err := s.selector.Select(bucket, key, obj.CurrentTier, cluster, false)
	if err != nil {
		return err
	}
	desiredReplicas := replicasFromNodes(desiredNodes, obj.CurrentTier)
	current := obj.ReplicasForTier(obj.CurrentTier)
	if sameReplicaSet(current, desiredReplicas) {
		return s.cleanupStaleTier(ctx, obj)
	}

	missing := diffReplicas(desiredReplicas, current)
	if len(missing) > 0 {
		tmp, cleanup, err := s.downloadObjectToTemp(ctx, sourceReplicas(obj, obj.CurrentTier), obj.ObjectID, obj.SHA256)
		if err != nil {
			return err
		}
		defer cleanup()
		if err := s.uploadReplicas(ctx, tmp.path, tmp.size, obj.SHA256, obj.ObjectID, missing); err != nil {
			return err
		}
	}

	_, err = s.store.PatchObject(ctx, replicasPatch(bucket, key, obj.ObjectID, obj.CurrentTier, desiredReplicas))
	if err != nil {
		return err
	}
	for _, extra := range diffReplicas(current, desiredReplicas) {
		_ = s.client.DeleteObject(ctx, extra.URL, extra.Tier, obj.ObjectID)
	}
	return s.cleanupStaleTier(ctx, obj)
}

func (s *Server) cleanupStaleTier(ctx context.Context, obj *model.Object) error {
	fresh, err := s.store.GetObject(obj.Bucket, obj.Key)
	if err != nil {
		return err
	}
	var staleTier model.Tier
	var stale []model.Replica
	switch fresh.CurrentTier {
	case model.TierHot:
		staleTier = model.TierWarm
		stale = fresh.WarmReplicas
	case model.TierWarm:
		staleTier = model.TierHot
		stale = fresh.HotReplicas
	default:
		return nil
	}
	if len(stale) == 0 {
		return nil
	}
	for _, rep := range stale {
		_ = s.client.DeleteObject(ctx, rep.URL, staleTier, fresh.ObjectID)
	}
	_, _ = s.store.PatchObject(ctx, replicasPatch(fresh.Bucket, fresh.Key, fresh.ObjectID, staleTier, nil))
	return nil
}

type downloadedObject struct {
	path string
	size int64
}

func (s *Server) downloadObjectToTemp(ctx context.Context, replicas []model.Replica, objectID, wantSHA256 string) (*downloadedObject, func(), error) {
	var lastErr error
	for _, rep := range replicas {
		resp, err := s.client.GetObject(ctx, rep.URL, rep.Tier, objectID)
		if err != nil {
			lastErr = err
			continue
		}
		tmp, err := os.CreateTemp(s.tmpDir, "tierstore-stage-*.bin")
		if err != nil {
			resp.Body.Close()
			return nil, nil, err
		}
		h := sha256.New()
		size, copyErr := io.Copy(io.MultiWriter(tmp, h), resp.Body)
		resp.Body.Close()
		closeErr := tmp.Close()
		sum := hex.EncodeToString(h.Sum(nil))
		if copyErr != nil {
			_ = os.Remove(tmp.Name())
			lastErr = copyErr
			continue
		}
		if closeErr != nil {
			_ = os.Remove(tmp.Name())
			lastErr = closeErr
			continue
		}
		if wantSHA256 != "" && wantSHA256 != sum {
			_ = os.Remove(tmp.Name())
			lastErr = fmt.Errorf("checksum mismatch while staging object: got=%s want=%s", sum, wantSHA256)
			continue
		}
		cleanup := func() { _ = os.Remove(tmp.Name()) }
		return &downloadedObject{path: tmp.Name(), size: size}, cleanup, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no readable replicas for object %s", objectID)
	}
	return nil, nil, lastErr
}

func sourceReplicas(obj *model.Object, preferred model.Tier) []model.Replica {
	var reps []model.Replica
	reps = append(reps, obj.ReplicasForTier(preferred)...)
	if preferred == model.TierHot {
		reps = append(reps, obj.ReplicasForTier(model.TierWarm)...)
	} else {
		reps = append(reps, obj.ReplicasForTier(model.TierHot)...)
	}
	seen := make(map[string]struct{})
	out := make([]model.Replica, 0, len(reps))
	for _, rep := range reps {
		key := string(rep.Tier) + "|" + rep.NodeID + "|" + rep.URL
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, rep)
	}
	return out
}

func sameReplicaSet(a, b []model.Replica) bool {
	if len(a) != len(b) {
		return false
	}
	ac := normalizedReplicaKeys(a)
	bc := normalizedReplicaKeys(b)
	for i := range ac {
		if ac[i] != bc[i] {
			return false
		}
	}
	return true
}

func diffReplicas(want, have []model.Replica) []model.Replica {
	haveSet := make(map[string]struct{}, len(have))
	for _, rep := range have {
		haveSet[repKey(rep)] = struct{}{}
	}
	var out []model.Replica
	for _, rep := range want {
		if _, ok := haveSet[repKey(rep)]; ok {
			continue
		}
		out = append(out, rep)
	}
	return out
}

func normalizedReplicaKeys(in []model.Replica) []string {
	out := make([]string, 0, len(in))
	for _, rep := range in {
		out = append(out, repKey(rep))
	}
	sort.Strings(out)
	return out
}

func repKey(rep model.Replica) string {
	return fmt.Sprintf("%s|%s|%s", rep.NodeID, rep.URL, rep.Tier)
}

func replicasPatch(bucket, key, objectID string, tier model.Tier, replicas []model.Replica) *meta.ObjectPatch {
	patch := &meta.ObjectPatch{
		Bucket:           bucket,
		Key:              key,
		ExpectedObjectID: objectID,
	}
	switch tier {
	case model.TierHot:
		patch.ReplaceHotReplicas = true
		patch.HotReplicas = cloneReplicaSet(replicas)
	case model.TierWarm:
		patch.ReplaceWarmReplicas = true
		patch.WarmReplicas = cloneReplicaSet(replicas)
	}
	return patch
}

func cloneReplicaSet(in []model.Replica) []model.Replica {
	if len(in) == 0 {
		return nil
	}
	out := make([]model.Replica, len(in))
	copy(out, in)
	return out
}
