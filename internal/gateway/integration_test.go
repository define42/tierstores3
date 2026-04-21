package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"tierstore/internal/meta"
	"tierstore/internal/node"
)

func TestGatewayClusterForwardingAndFailover(t *testing.T) {
	nodes := []*storageNodeHarness{
		startStorageNode(t, "node1"),
		startStorageNode(t, "node2"),
		startStorageNode(t, "node3"),
	}
	for _, n := range nodes {
		defer n.Close()
	}

	httpAddrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	raftAddrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	peers := []meta.Peer{
		{ID: "gw1", APIURL: "http://" + httpAddrs[0], RaftAddr: raftAddrs[0]},
		{ID: "gw2", APIURL: "http://" + httpAddrs[1], RaftAddr: raftAddrs[1]},
		{ID: "gw3", APIURL: "http://" + httpAddrs[2], RaftAddr: raftAddrs[2]},
	}
	gateways := []*gatewayHarness{
		startGateway(t, Config{
			GatewayID:         "gw1",
			AdvertiseURL:      peers[0].APIURL,
			RaftAddr:          peers[0].RaftAddr,
			RaftDir:           t.TempDir(),
			Bootstrap:         true,
			ControllerPeers:   peers,
			ReplicationFactor: 2,
			ColdAfter:         24 * time.Hour,
		}),
		startGateway(t, Config{
			GatewayID:         "gw2",
			AdvertiseURL:      peers[1].APIURL,
			RaftAddr:          peers[1].RaftAddr,
			RaftDir:           t.TempDir(),
			ControllerPeers:   peers,
			ReplicationFactor: 2,
			ColdAfter:         24 * time.Hour,
		}),
		startGateway(t, Config{
			GatewayID:         "gw3",
			AdvertiseURL:      peers[2].APIURL,
			RaftAddr:          peers[2].RaftAddr,
			RaftDir:           t.TempDir(),
			ControllerPeers:   peers,
			ReplicationFactor: 2,
			ColdAfter:         24 * time.Hour,
		}),
	}
	defer func() {
		for _, gw := range gateways {
			gw.Close()
		}
	}()
	waitForClusterReady(t, gateways)

	client := &http.Client{Timeout: 10 * time.Second}
	for _, n := range nodes {
		adminAddNode(t, client, gateways[1].url, n)
	}

	putEmpty(t, client, gateways[1].url+"/photos")
	putBytes(t, client, gateways[1].url+"/photos/2026/trip/pic.jpg", []byte("hello-ha"))
	got := getBytes(t, client, gateways[2].url+"/photos/2026/trip/pic.jpg")
	if string(got) != "hello-ha" {
		t.Fatalf("unexpected object body: %q", string(got))
	}
	listing := getText(t, client, gateways[0].url+"/photos?list-type=2")
	if !strings.Contains(listing, "<Key>2026/trip/pic.jpg</Key>") {
		t.Fatalf("listing missing object: %s", listing)
	}

	leader := currentLeader(t, gateways)
	leader.Close()
	waitForNewLeader(t, gateways, leader.id)
	alive := aliveGateways(gateways)
	if len(alive) < 2 {
		t.Fatalf("need two surviving gateways after failover, got %d", len(alive))
	}

	putEmpty(t, client, alive[0].url+"/docs")
	putBytes(t, client, alive[0].url+"/docs/readme.txt", []byte("after-failover"))
	got = getBytes(t, client, alive[1].url+"/docs/readme.txt")
	if string(got) != "after-failover" {
		t.Fatalf("unexpected post-failover body: %q", string(got))
	}
	deleteRequest(t, client, alive[1].url+"/docs/readme.txt")
	resp, err := client.Get(alive[0].url + "/docs/readme.txt")
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", resp.StatusCode)
	}
}

type gatewayHarness struct {
	id       string
	url      string
	server   *Server
	httpSrv  *http.Server
	listener net.Listener
	cancel   context.CancelFunc
	closed   bool
}

func startGateway(t *testing.T, cfg Config) *gatewayHarness {
	t.Helper()
	u, err := url.Parse(cfg.AdvertiseURL)
	if err != nil {
		t.Fatalf("parse advertise url: %v", err)
	}
	ln, err := net.Listen("tcp", u.Host)
	if err != nil {
		t.Fatalf("listen gateway: %v", err)
	}
	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	srv.RunBackground(ctx)
	httpSrv := &http.Server{Handler: srv.Handler()}
	go func() {
		_ = httpSrv.Serve(ln)
	}()
	return &gatewayHarness{
		id:       cfg.GatewayID,
		url:      cfg.AdvertiseURL,
		server:   srv,
		httpSrv:  httpSrv,
		listener: ln,
		cancel:   cancel,
	}
}

func (g *gatewayHarness) Close() {
	if g == nil || g.closed {
		return
	}
	g.closed = true
	g.cancel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = g.httpSrv.Shutdown(ctx)
	_ = g.listener.Close()
	_ = g.server.Close()
}

type storageNodeHarness struct {
	id     string
	url    string
	server *http.Server
}

func startStorageNode(t *testing.T, id string) *storageNodeHarness {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen storage node: %v", err)
	}
	srv := &http.Server{Handler: node.New(id, t.TempDir(), t.TempDir()).Handler()}
	go func() {
		_ = srv.Serve(ln)
	}()
	return &storageNodeHarness{id: id, url: "http://" + ln.Addr().String(), server: srv}
}

func (n *storageNodeHarness) Close() {
	if n == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = n.server.Shutdown(ctx)
}

func adminAddNode(t *testing.T, client *http.Client, gatewayURL string, node *storageNodeHarness) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"id":          node.id,
		"url":         node.url,
		"weight_hot":  1,
		"weight_warm": 1,
	})
	if err != nil {
		t.Fatalf("marshal admin node: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, gatewayURL+"/_admin/nodes", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new admin request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("post admin node: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected admin node status %d: %s", resp.StatusCode, string(data))
	}
}

func waitForClusterReady(t *testing.T, gateways []*gatewayHarness) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		ready := true
		for _, gw := range gateways {
			if gw.closed {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			err := gw.server.store.Barrier(ctx)
			cancel()
			if err != nil {
				ready = false
				break
			}
		}
		if ready && currentLeaderOrNil(gateways) != nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("cluster did not become ready")
}

func currentLeader(t *testing.T, gateways []*gatewayHarness) *gatewayHarness {
	t.Helper()
	leader := currentLeaderOrNil(gateways)
	if leader == nil {
		t.Fatalf("no leader found")
	}
	return leader
}

func currentLeaderOrNil(gateways []*gatewayHarness) *gatewayHarness {
	for _, gw := range gateways {
		if gw.closed {
			continue
		}
		if gw.server.store.IsLeader() {
			return gw
		}
	}
	return nil
}

func waitForNewLeader(t *testing.T, gateways []*gatewayHarness, previousID string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		leader := currentLeaderOrNil(gateways)
		if leader != nil && leader.id != previousID {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("new leader was not elected")
}

func putEmpty(t *testing.T, client *http.Client, target string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, target, nil)
	if err != nil {
		t.Fatalf("new put request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("put request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected put status %d: %s", resp.StatusCode, string(data))
	}
}

func putBytes(t *testing.T, client *http.Client, target string, body []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, target, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new put bytes request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("put bytes: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected put bytes status %d: %s", resp.StatusCode, string(data))
	}
}

func getBytes(t *testing.T, client *http.Client, target string) []byte {
	t.Helper()
	resp, err := client.Get(target)
	if err != nil {
		t.Fatalf("get request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected get status %d: %s", resp.StatusCode, string(data))
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read get body: %v", err)
	}
	return data
}

func getText(t *testing.T, client *http.Client, target string) string {
	t.Helper()
	return string(getBytes(t, client, target))
}

func deleteRequest(t *testing.T, client *http.Client, target string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, target, nil)
	if err != nil {
		t.Fatalf("new delete request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("delete request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected delete status %d: %s", resp.StatusCode, string(data))
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for free addr: %v", err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func aliveGateways(gateways []*gatewayHarness) []*gatewayHarness {
	out := make([]*gatewayHarness, 0, len(gateways))
	for _, gw := range gateways {
		if gw == nil || gw.closed {
			continue
		}
		out = append(out, gw)
	}
	return out
}

func (g *gatewayHarness) String() string {
	return fmt.Sprintf("%s(%s)", g.id, g.url)
}
