package gateway

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"tierstore/internal/lockset"
	"tierstore/internal/meta"
	"tierstore/internal/model"
	"tierstore/internal/placement"
	"tierstore/internal/storageclient"
)

type Config struct {
	StatePath           string
	PlacementGroups     uint64
	ReplicationFactor   int
	ColdAfter           time.Duration
	AccessFlushInterval time.Duration
	TierScanInterval    time.Duration
	RebalanceInterval   time.Duration
	PromoteOnRead       bool
	NodeHTTPTimeout     time.Duration
}

type Server struct {
	cfg      Config
	store    *meta.Store
	selector *placement.Selector
	client   *storageclient.Client
	locks    *lockset.Set
	accessCh chan accessEvent
	tmpDir   string
}

type accessEvent struct {
	bucket string
	key    string
	at     time.Time
}

func New(cfg Config) (*Server, error) {
	if cfg.PlacementGroups == 0 {
		cfg.PlacementGroups = 1024
	}
	if cfg.ReplicationFactor == 0 {
		cfg.ReplicationFactor = 3
	}
	if cfg.ColdAfter == 0 {
		cfg.ColdAfter = 30 * 24 * time.Hour
	}
	if cfg.AccessFlushInterval == 0 {
		cfg.AccessFlushInterval = 5 * time.Second
	}
	if cfg.TierScanInterval == 0 {
		cfg.TierScanInterval = time.Minute
	}
	if cfg.RebalanceInterval == 0 {
		cfg.RebalanceInterval = 2 * time.Minute
	}
	if cfg.NodeHTTPTimeout == 0 {
		cfg.NodeHTTPTimeout = 2 * time.Minute
	}
	store, err := meta.NewStore(cfg.StatePath, cfg.PlacementGroups, cfg.ReplicationFactor)
	if err != nil {
		return nil, err
	}
	return &Server{
		cfg:      cfg,
		store:    store,
		selector: placement.NewSelector(),
		client:   storageclient.New(cfg.NodeHTTPTimeout),
		locks:    lockset.New(),
		accessCh: make(chan accessEvent, 4096),
		tmpDir:   os.TempDir(),
	}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/_admin/cluster", s.handleAdminCluster)
	mux.HandleFunc("/_admin/nodes", s.handleAdminNodes)
	mux.HandleFunc("/_admin/nodes/", s.handleAdminNodeAction)
	mux.HandleFunc("/", s.handleRoot)
	return mux
}

func (s *Server) RunBackground(ctx context.Context) {
	go s.runAccessTracker(ctx)
	go s.runTieringLoop(ctx)
	go s.runRebalanceLoop(ctx)
}

func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	bucket, key := splitBucketKey(r.URL.Path)
	switch {
	case bucket == "" && r.Method == http.MethodGet:
		s.handleListBuckets(w, r)
	case bucket != "" && key == "" && r.Method == http.MethodPut:
		s.handleCreateBucket(w, r, bucket)
	case bucket != "" && key == "" && r.Method == http.MethodGet:
		s.handleListObjects(w, r, bucket)
	case bucket != "" && key != "":
		s.handleObject(w, r, bucket, key)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleCreateBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if err := s.store.EnsureBucket(bucket); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	switch r.Method {
	case http.MethodPut:
		s.handlePutObject(w, r, bucket, key)
	case http.MethodGet:
		s.handleGetObject(w, r, bucket, key)
	case http.MethodHead:
		s.handleHeadObject(w, r, bucket, key)
	case http.MethodDelete:
		s.handleDeleteObject(w, r, bucket, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handlePutObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if err := s.store.EnsureBucket(bucket); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	cluster := s.store.Snapshot().Cluster
	nodes, pg, err := s.selector.Select(bucket, key, model.TierHot, cluster, false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	spooled, cleanup, err := spoolBody(r.Body, s.tmpDir)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer cleanup()

	objectID := newObjectID()
	replicas := replicasFromNodes(nodes, model.TierHot)
	if err := s.uploadReplicas(r.Context(), spooled.path, spooled.size, spooled.sha256, objectID, replicas); err != nil {
		s.bestEffortDeleteReplicas(context.Background(), replicas, objectID)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	now := time.Now().UTC()
	obj := &model.Object{
		Bucket:         bucket,
		Key:            key,
		ObjectID:       objectID,
		Size:           spooled.size,
		ETag:           fmt.Sprintf("\"%s\"", spooled.md5),
		SHA256:         spooled.sha256,
		PlacementGroup: pg,
		CurrentTier:    model.TierHot,
		HotReplicas:    replicas,
		CreatedAt:      now,
		UpdatedAt:      now,
		LastAccessedAt: now,
	}
	old, _ := s.store.GetObject(bucket, key)
	if err := s.store.PutObject(obj); err != nil {
		s.bestEffortDeleteReplicas(context.Background(), replicas, objectID)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if old != nil && old.ObjectID != objectID {
		go s.bestEffortDeleteRecordCopies(context.Background(), old)
	}
	w.Header().Set("ETag", obj.ETag)
	w.Header().Set("X-Placement-Group", strconv.FormatUint(pg, 10))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	obj, err := s.store.GetObject(bucket, key)
	if err != nil {
		status := http.StatusNotFound
		if !errors.Is(err, meta.ErrNotFound) {
			status = http.StatusInternalServerError
		}
		http.Error(w, err.Error(), status)
		return
	}
	resp, err := s.fetchFromAnyReplica(r.Context(), obj, obj.CurrentTier, http.MethodGet)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.Header().Set("ETag", obj.ETag)
	w.Header().Set("Last-Modified", obj.UpdatedAt.UTC().Format(http.TimeFormat))
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		w.Header().Set("Content-Length", cl)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, resp.Body); err != nil {
		return
	}
	s.recordAccess(bucket, key)
	if obj.CurrentTier == model.TierWarm && s.cfg.PromoteOnRead {
		go s.promoteObject(context.Background(), bucket, key)
	}
}

func (s *Server) handleHeadObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	obj, err := s.store.GetObject(bucket, key)
	if err != nil {
		status := http.StatusNotFound
		if !errors.Is(err, meta.ErrNotFound) {
			status = http.StatusInternalServerError
		}
		http.Error(w, err.Error(), status)
		return
	}
	resp, err := s.fetchFromAnyReplica(r.Context(), obj, obj.CurrentTier, http.MethodHead)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.Header().Set("ETag", obj.ETag)
	w.Header().Set("Last-Modified", obj.UpdatedAt.UTC().Format(http.TimeFormat))
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		w.Header().Set("Content-Length", cl)
	} else {
		w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	}
	w.WriteHeader(http.StatusOK)
	s.recordAccess(bucket, key)
}

func (s *Server) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	obj, err := s.store.DeleteObject(bucket, key)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	go s.bestEffortDeleteRecordCopies(context.Background(), obj)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleAdminCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, s.store.Snapshot())
}

func (s *Server) handleAdminNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var in struct {
		ID         string  `json:"id"`
		URL        string  `json:"url"`
		WeightHot  float64 `json:"weight_hot"`
		WeightWarm float64 `json:"weight_warm"`
		Zone       string  `json:"zone"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, fmt.Sprintf("decode json: %v", err), http.StatusBadRequest)
		return
	}
	if in.ID == "" || in.URL == "" {
		http.Error(w, "id and url are required", http.StatusBadRequest)
		return
	}
	node := &model.Node{ID: in.ID, URL: strings.TrimRight(in.URL, "/"), WeightHot: in.WeightHot, WeightWarm: in.WeightWarm, Zone: in.Zone, State: model.NodeStateActive}
	if err := s.store.UpsertNode(node); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, node)
}

func (s *Server) handleAdminNodeAction(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/_admin/nodes/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	id := parts[0]
	if len(parts) == 1 && r.Method == http.MethodDelete {
		if err := s.store.RemoveNode(id); err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, meta.ErrNotFound) {
				status = http.StatusNotFound
			}
			http.Error(w, err.Error(), status)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if len(parts) != 2 || r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	var state model.NodeState
	switch parts[1] {
	case "drain":
		state = model.NodeStateDraining
	case "activate":
		state = model.NodeStateActive
	default:
		http.NotFound(w, r)
		return
	}
	if err := s.store.SetNodeState(id, state); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, meta.ErrNotFound) {
			status = http.StatusNotFound
		}
		http.Error(w, err.Error(), status)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) fetchFromAnyReplica(ctx context.Context, obj *model.Object, tier model.Tier, method string) (*http.Response, error) {
	var lastErr error
	for _, rep := range obj.ReplicasForTier(tier) {
		var (
			resp *http.Response
			err  error
		)
		switch method {
		case http.MethodHead:
			resp, err = s.client.HeadObject(ctx, rep.URL, tier, obj.ObjectID)
		default:
			resp, err = s.client.GetObject(ctx, rep.URL, tier, obj.ObjectID)
		}
		if err == nil {
			return resp, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no replicas for %s/%s", obj.Bucket, obj.Key)
	}
	return nil, lastErr
}

func (s *Server) uploadReplicas(ctx context.Context, filePath string, size int64, sha256sum, objectID string, replicas []model.Replica) error {
	for _, rep := range replicas {
		if err := s.client.UploadFile(ctx, rep.URL, rep.Tier, objectID, filePath, sha256sum, size); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) bestEffortDeleteReplicas(ctx context.Context, replicas []model.Replica, objectID string) {
	for _, rep := range replicas {
		_ = s.client.DeleteObject(ctx, rep.URL, rep.Tier, objectID)
	}
}

func (s *Server) bestEffortDeleteRecordCopies(ctx context.Context, obj *model.Object) {
	if obj == nil {
		return
	}
	s.bestEffortDeleteReplicas(ctx, obj.HotReplicas, obj.ObjectID)
	s.bestEffortDeleteReplicas(ctx, obj.WarmReplicas, obj.ObjectID)
}

func (s *Server) recordAccess(bucket, key string) {
	select {
	case s.accessCh <- accessEvent{bucket: bucket, key: key, at: time.Now().UTC()}:
	default:
		go func() {
			select {
			case s.accessCh <- accessEvent{bucket: bucket, key: key, at: time.Now().UTC()}:
			case <-time.After(250 * time.Millisecond):
			}
		}()
	}
}

type spooledBody struct {
	path   string
	size   int64
	sha256 string
	md5    string
}

func spoolBody(r io.Reader, dir string) (*spooledBody, func(), error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("mkdir temp dir: %w", err)
	}
	f, err := os.CreateTemp(dir, "tierstore-upload-*.bin")
	if err != nil {
		return nil, nil, fmt.Errorf("create temp file: %w", err)
	}
	defer f.Close()
	sha := sha256.New()
	md5h := md5.New()
	size, err := io.Copy(io.MultiWriter(f, sha, md5h), r)
	if err != nil {
		_ = os.Remove(f.Name())
		return nil, nil, fmt.Errorf("spool request body: %w", err)
	}
	cleanup := func() { _ = os.Remove(f.Name()) }
	return &spooledBody{
		path:   f.Name(),
		size:   size,
		sha256: hex.EncodeToString(sha.Sum(nil)),
		md5:    hex.EncodeToString(md5h.Sum(nil)),
	}, cleanup, nil
}

func newObjectID() string {
	buf := make([]byte, 18)
	_, _ = rand.Read(buf)
	return strings.TrimRight(base64.RawURLEncoding.EncodeToString(buf), "=")
}

func splitBucketKey(p string) (bucket, key string) {
	trimmed := strings.TrimPrefix(pathClean(p), "/")
	if trimmed == "" {
		return "", ""
	}
	parts := strings.SplitN(trimmed, "/", 2)
	bucket = parts[0]
	if len(parts) == 2 {
		key = parts[1]
	}
	return bucket, key
}

func pathClean(p string) string {
	if p == "" {
		return "/"
	}
	return filepath.ToSlash(filepath.Clean(p))
}

func replicasFromNodes(nodes []*model.Node, tier model.Tier) []model.Replica {
	replicas := make([]model.Replica, 0, len(nodes))
	now := time.Now().UTC()
	for _, node := range nodes {
		replicas = append(replicas, model.Replica{NodeID: node.ID, URL: node.URL, Tier: tier, AddedAt: now})
	}
	sort.Slice(replicas, func(i, j int) bool { return replicas[i].NodeID < replicas[j].NodeID })
	return replicas
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
