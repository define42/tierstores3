package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"tierstore/internal/gateway"
	"tierstore/internal/meta"
	"tierstore/internal/node"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "node":
		runNode(os.Args[2:])
	case "gateway":
		runGateway(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func runNode(args []string) {
	fs := flag.NewFlagSet("node", flag.ExitOnError)
	var (
		id      = fs.String("id", "", "node ID")
		listen  = fs.String("listen", ":9100", "listen address")
		hotDir  = fs.String("hot-dir", "./data/hot", "NVMe/hot data directory")
		warmDir = fs.String("warm-dir", "./data/warm", "HDD/warm data directory")
	)
	fs.Parse(args)
	if *id == "" {
		log.Fatal("-id is required")
	}
	if err := os.MkdirAll(*hotDir, 0o755); err != nil {
		log.Fatalf("mkdir hot dir: %v", err)
	}
	if err := os.MkdirAll(*warmDir, 0o755); err != nil {
		log.Fatalf("mkdir warm dir: %v", err)
	}
	server := &http.Server{Addr: *listen, Handler: node.New(*id, *hotDir, *warmDir).Handler()}
	go func() {
		<-signalContext().Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()
	log.Printf("node %s listening on %s hot=%s warm=%s", *id, *listen, *hotDir, *warmDir)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func runGateway(args []string) {
	fs := flag.NewFlagSet("gateway", flag.ExitOnError)
	var (
		listen          = fs.String("listen", ":9000", "listen address")
		gatewayID       = fs.String("gateway-id", "", "gateway/controller node ID")
		advertiseURL    = fs.String("advertise-url", "", "gateway API URL advertised to peer gateways")
		raftAddr        = fs.String("raft-addr", "", "raft bind address for this gateway")
		raftDir         = fs.String("raft-dir", "./raft", "raft state directory")
		join            = fs.String("join", "", "comma-separated peer list: id=http://api:port@raft-host:port")
		bootstrap       = fs.Bool("bootstrap", false, "bootstrap the controller cluster and add configured peers")
		placementGroups = fs.Uint64("placement-groups", 1024, "number of placement groups")
		replicas        = fs.Int("replicas", 3, "replication factor")
		coldAfter       = fs.Duration("cold-after", 30*24*time.Hour, "move objects to warm tier after this idle period")
		promoteOnRead   = fs.Bool("promote-on-read", true, "promote warm objects back to hot on read")
		scanInterval    = fs.Duration("tier-scan-interval", time.Minute, "tiering scan interval")
		rebalanceInt    = fs.Duration("rebalance-interval", 2*time.Minute, "rebalance scan interval")
	)
	fs.Parse(args)
	if *gatewayID == "" {
		log.Fatal("-gateway-id is required")
	}
	if *raftAddr == "" {
		log.Fatal("-raft-addr is required")
	}
	if *advertiseURL == "" {
		*advertiseURL = defaultAdvertiseURL(*listen)
	}
	peers, err := parsePeers(*join)
	if err != nil {
		log.Fatal(err)
	}

	srv, err := gateway.New(gateway.Config{
		GatewayID:         *gatewayID,
		AdvertiseURL:      *advertiseURL,
		RaftAddr:          *raftAddr,
		RaftDir:           *raftDir,
		Bootstrap:         *bootstrap,
		ControllerPeers:   peers,
		PlacementGroups:   *placementGroups,
		ReplicationFactor: *replicas,
		ColdAfter:         *coldAfter,
		TierScanInterval:  *scanInterval,
		RebalanceInterval: *rebalanceInt,
		PromoteOnRead:     *promoteOnRead,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := srv.Close(); err != nil {
			log.Printf("close gateway store: %v", err)
		}
	}()
	ctx := signalContext()
	srv.RunBackground(ctx)
	httpSrv := &http.Server{Addr: *listen, Handler: srv.Handler()}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}()
	log.Printf("gateway %s listening on %s api=%s raft=%s dir=%s", *gatewayID, *listen, *advertiseURL, *raftAddr, *raftDir)
	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func signalContext() context.Context {
	ctx, _ := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	return ctx
}

func usage() {
	fmt.Fprintln(os.Stderr, `tierstore: hot/warm replicated S3-like object store prototype

Usage:
  tierstore node    -id node1 -listen :9101 -hot-dir /nvme -warm-dir /hdd
  tierstore gateway -gateway-id gw1 -listen :9000 -advertise-url http://127.0.0.1:9000 -raft-addr 127.0.0.1:10001 -raft-dir ./gw1-raft`)
}

func parsePeers(raw string) ([]meta.Peer, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	peers := make([]meta.Peer, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, rest, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("invalid peer %q: want id=http://api:port@raft-host:port", part)
		}
		apiURL, raftAddr, ok := strings.Cut(rest, "@")
		if !ok {
			return nil, fmt.Errorf("invalid peer %q: want id=http://api:port@raft-host:port", part)
		}
		if _, err := url.Parse(apiURL); err != nil {
			return nil, fmt.Errorf("invalid peer api url %q: %w", apiURL, err)
		}
		peers = append(peers, meta.Peer{
			ID:       strings.TrimSpace(id),
			APIURL:   strings.TrimRight(strings.TrimSpace(apiURL), "/"),
			RaftAddr: strings.TrimSpace(raftAddr),
		})
	}
	return peers, nil
}

func defaultAdvertiseURL(listen string) string {
	hostPort := listen
	if strings.HasPrefix(hostPort, ":") {
		hostPort = "127.0.0.1" + hostPort
	}
	return "http://" + hostPort
}
