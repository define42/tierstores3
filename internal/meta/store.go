package meta

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"tierstore/internal/model"
)

var (
	ErrNotFound  = errors.New("not found")
	ErrConflict  = errors.New("conflict")
	ErrNotLeader = errors.New("not leader")
)

type Peer struct {
	ID       string `json:"id"`
	APIURL   string `json:"api_url"`
	RaftAddr string `json:"raft_addr"`
}

type Config struct {
	GatewayID         string
	AdvertiseURL      string
	RaftAddr          string
	RaftDir           string
	Bootstrap         bool
	Peers             []Peer
	PlacementGroups   uint64
	ReplicationFactor int
	ApplyTimeout      time.Duration
	ReadTimeout       time.Duration
	HTTPClient        *http.Client
}

type Store struct {
	cfg        Config
	fsm        *fsm
	raft       *raft.Raft
	logStore   *raftboltdb.BoltStore
	transport  *raft.NetworkTransport
	httpClient *http.Client

	mu       sync.RWMutex
	peerByID map[string]Peer

	ctx    context.Context
	cancel context.CancelFunc
}

func NewStore(cfg Config) (*Store, error) {
	if cfg.GatewayID == "" {
		return nil, fmt.Errorf("gateway id is required")
	}
	if cfg.AdvertiseURL == "" {
		return nil, fmt.Errorf("advertise url is required")
	}
	if cfg.RaftAddr == "" {
		return nil, fmt.Errorf("raft addr is required")
	}
	if cfg.RaftDir == "" {
		return nil, fmt.Errorf("raft dir is required")
	}
	if cfg.PlacementGroups == 0 {
		cfg.PlacementGroups = 1024
	}
	if cfg.ReplicationFactor == 0 {
		cfg.ReplicationFactor = 3
	}
	if cfg.ApplyTimeout == 0 {
		cfg.ApplyTimeout = 10 * time.Second
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 5 * time.Second
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: cfg.ReadTimeout}
	}
	if _, err := url.Parse(cfg.AdvertiseURL); err != nil {
		return nil, fmt.Errorf("parse advertise url: %w", err)
	}

	cfg.Peers = normalizePeers(cfg)
	if err := os.MkdirAll(cfg.RaftDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir raft dir: %w", err)
	}

	fsm := newFSM(cfg.PlacementGroups, cfg.ReplicationFactor)
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.RaftDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("open raft bolt store: %w", err)
	}
	snapshots, err := raft.NewFileSnapshotStore(filepath.Join(cfg.RaftDir, "snapshots"), 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("open raft snapshot store: %w", err)
	}
	hasState, err := raft.HasExistingState(logStore, logStore, snapshots)
	if err != nil {
		return nil, fmt.Errorf("check raft state: %w", err)
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.GatewayID)
	raftConfig.SnapshotInterval = 30 * time.Second
	raftConfig.SnapshotThreshold = 64

	transport, err := raft.NewTCPTransport(cfg.RaftAddr, nil, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("create raft transport: %w", err)
	}
	if cfg.Bootstrap && !hasState {
		initial := raft.Configuration{
			Servers: []raft.Server{{
				ID:       raft.ServerID(cfg.GatewayID),
				Address:  transport.LocalAddr(),
				Suffrage: raft.Voter,
			}},
		}
		if err := raft.BootstrapCluster(raftConfig, logStore, logStore, snapshots, transport, initial); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			return nil, fmt.Errorf("bootstrap raft cluster: %w", err)
		}
	}

	r, err := raft.NewRaft(raftConfig, fsm, logStore, logStore, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("create raft node: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	store := &Store{
		cfg:        cfg,
		fsm:        fsm,
		raft:       r,
		logStore:   logStore,
		transport:  transport,
		httpClient: cfg.HTTPClient,
		peerByID:   make(map[string]Peer, len(cfg.Peers)),
		ctx:        ctx,
		cancel:     cancel,
	}
	for _, peer := range cfg.Peers {
		store.peerByID[peer.ID] = peer
	}
	if cfg.Bootstrap {
		go store.runBootstrapMembership()
	}
	return store, nil
}

func (s *Store) Close() error {
	s.cancel()
	var shutdownErr error
	if s.transport != nil {
		if err := s.transport.Close(); err != nil && shutdownErr == nil {
			shutdownErr = err
		}
	}
	if s.raft != nil {
		if err := s.raft.Shutdown().Error(); err != nil && shutdownErr == nil {
			shutdownErr = err
		}
	}
	if s.logStore != nil {
		if err := s.logStore.Close(); err != nil && shutdownErr == nil {
			shutdownErr = err
		}
	}
	return shutdownErr
}

func (s *Store) LeaderCh() <-chan bool {
	return s.raft.LeaderCh()
}

func (s *Store) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

func (s *Store) Snapshot() model.State {
	return s.fsm.snapshot()
}

func (s *Store) BucketExists(name string) bool {
	return s.fsm.bucketExists(name)
}

func (s *Store) GetObject(bucket, key string) (*model.Object, error) {
	return s.fsm.getObject(bucket, key)
}

func (s *Store) Barrier(ctx context.Context) error {
	if s.IsLeader() {
		return s.barrierOnLeader(ctx)
	}
	idx, err := s.fetchLeaderReadIndex(ctx)
	if err != nil {
		return err
	}
	return s.waitForAppliedIndex(ctx, idx)
}

func (s *Store) EnsureBucket(ctx context.Context, name string) error {
	return s.apply(ctx, command{
		Type:   commandEnsureBucket,
		Bucket: name,
		At:     time.Now().UTC(),
	}, nil)
}

func (s *Store) PutObject(ctx context.Context, obj *model.Object) (*model.Object, error) {
	result := &commandResult{}
	if err := s.apply(ctx, command{Type: commandPutObject, Object: model.CloneObject(obj)}, result); err != nil {
		return nil, err
	}
	return result.Object, nil
}

func (s *Store) DeleteObject(ctx context.Context, bucket, key string) (*model.Object, error) {
	result := &commandResult{}
	if err := s.apply(ctx, command{Type: commandDeleteObject, Bucket: bucket, Key: key}, result); err != nil {
		return nil, err
	}
	return result.Object, nil
}

func (s *Store) MarkAccessBatch(ctx context.Context, events []model.AccessEvent) error {
	if len(events) == 0 {
		return nil
	}
	if s.IsLeader() {
		return s.apply(ctx, command{Type: commandMarkAccesses, Events: cloneAccessEvents(events)}, nil)
	}
	return s.forwardAccessBatch(ctx, events)
}

func (s *Store) UpsertNode(ctx context.Context, node *model.Node) (*model.Node, error) {
	result := &commandResult{}
	if err := s.apply(ctx, command{
		Type: commandUpsertNode,
		Node: model.CloneNode(node),
		At:   time.Now().UTC(),
	}, result); err != nil {
		return nil, err
	}
	return result.Node, nil
}

func (s *Store) SetNodeState(ctx context.Context, id string, state model.NodeState) error {
	return s.apply(ctx, command{
		Type:      commandSetNodeState,
		NodeID:    id,
		NodeState: state,
		At:        time.Now().UTC(),
	}, nil)
}

func (s *Store) RemoveNode(ctx context.Context, id string) error {
	return s.apply(ctx, command{
		Type:   commandRemoveNode,
		NodeID: id,
		At:     time.Now().UTC(),
	}, nil)
}

func (s *Store) PatchObject(ctx context.Context, patch *ObjectPatch) (*model.Object, error) {
	result := &commandResult{}
	if err := s.apply(ctx, command{Type: commandPatchObject, ObjectPatch: patch}, result); err != nil {
		return nil, err
	}
	return result.Object, nil
}

func (s *Store) ReadIndex(ctx context.Context) (uint64, error) {
	if !s.IsLeader() {
		return 0, ErrNotLeader
	}
	if err := s.barrierOnLeader(ctx); err != nil {
		return 0, err
	}
	return s.raft.AppliedIndex(), nil
}

func (s *Store) LeaderPeer() (Peer, error) {
	leaderAddr, leaderID := s.raft.LeaderWithID()
	if leaderID == "" {
		return Peer{}, ErrNotLeader
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if peer, ok := s.peerByID[string(leaderID)]; ok {
		return peer, nil
	}
	return Peer{ID: string(leaderID), RaftAddr: string(leaderAddr)}, nil
}

func (s *Store) apply(ctx context.Context, cmd command, out *commandResult) error {
	if !s.IsLeader() {
		return ErrNotLeader
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}
	future := s.raft.Apply(data, timeoutFromContext(ctx, s.cfg.ApplyTimeout))
	if err := future.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return ErrNotLeader
		}
		return err
	}
	resp, _ := future.Response().(*commandResult)
	if resp == nil {
		return nil
	}
	if err := commandError(resp.Err); err != nil {
		return err
	}
	if out != nil {
		*out = *resp
	}
	return nil
}

func (s *Store) barrierOnLeader(ctx context.Context) error {
	future := s.raft.Barrier(timeoutFromContext(ctx, s.cfg.ReadTimeout))
	if err := future.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return ErrNotLeader
		}
		return err
	}
	return nil
}

func (s *Store) fetchLeaderReadIndex(ctx context.Context) (uint64, error) {
	peer, err := s.LeaderPeer()
	if err != nil {
		return 0, err
	}
	if peer.APIURL == "" {
		return 0, fmt.Errorf("leader api url is unknown")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(peer.APIURL, "/")+"/_internal/control/read-index", nil)
	if err != nil {
		return 0, err
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusConflict {
		return 0, ErrNotLeader
	}
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return 0, fmt.Errorf("leader read-index failed: status=%d body=%q", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var payload struct {
		AppliedIndex uint64 `json:"applied_index"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return 0, err
	}
	return payload.AppliedIndex, nil
}

func (s *Store) waitForAppliedIndex(ctx context.Context, want uint64) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if s.raft.AppliedIndex() >= want {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Store) forwardAccessBatch(ctx context.Context, events []model.AccessEvent) error {
	peer, err := s.LeaderPeer()
	if err != nil {
		return err
	}
	body, err := json.Marshal(struct {
		Events []model.AccessEvent `json:"events"`
	}{
		Events: cloneAccessEvents(events),
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(peer.APIURL, "/")+"/_internal/control/access-batch", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusConflict {
		return ErrNotLeader
	}
	if resp.StatusCode/100 != 2 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return fmt.Errorf("forward access batch failed: status=%d body=%q", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return nil
}

func (s *Store) runBootstrapMembership() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if !s.IsLeader() {
				continue
			}
			if err := s.ensureConfiguredPeers(); err != nil {
				log.Printf("raft membership reconcile: %v", err)
			}
		}
	}
}

func (s *Store) ensureConfiguredPeers() error {
	future := s.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return err
	}
	current := make(map[raft.ServerID]raft.Server, len(future.Configuration().Servers))
	for _, server := range future.Configuration().Servers {
		current[server.ID] = server
	}
	s.mu.RLock()
	peers := make([]Peer, 0, len(s.peerByID))
	for _, peer := range s.peerByID {
		peers = append(peers, peer)
	}
	s.mu.RUnlock()
	for _, peer := range peers {
		if peer.ID == s.cfg.GatewayID {
			continue
		}
		serverID := raft.ServerID(peer.ID)
		if existing, ok := current[serverID]; ok && string(existing.Address) == peer.RaftAddr {
			continue
		}
		future := s.raft.AddVoter(serverID, raft.ServerAddress(peer.RaftAddr), 0, s.cfg.ApplyTimeout)
		if err := future.Error(); err != nil && !strings.Contains(err.Error(), "exists") {
			return err
		}
	}
	return nil
}

func normalizePeers(cfg Config) []Peer {
	seen := make(map[string]Peer)
	for _, peer := range cfg.Peers {
		if peer.ID == "" {
			continue
		}
		peer.APIURL = strings.TrimRight(peer.APIURL, "/")
		seen[peer.ID] = peer
	}
	seen[cfg.GatewayID] = Peer{
		ID:       cfg.GatewayID,
		APIURL:   strings.TrimRight(cfg.AdvertiseURL, "/"),
		RaftAddr: cfg.RaftAddr,
	}
	out := make([]Peer, 0, len(seen))
	for _, peer := range seen {
		out = append(out, peer)
	}
	return out
}

func cloneAccessEvents(events []model.AccessEvent) []model.AccessEvent {
	if len(events) == 0 {
		return nil
	}
	out := make([]model.AccessEvent, len(events))
	copy(out, events)
	return out
}

func timeoutFromContext(ctx context.Context, fallback time.Duration) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		timeout := time.Until(deadline)
		if timeout <= 0 {
			return time.Millisecond
		}
		return timeout
	}
	return fallback
}
