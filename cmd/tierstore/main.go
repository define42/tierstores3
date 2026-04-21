package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tierstore/internal/gateway"
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
		statePath       = fs.String("state", "./tierstore-state.json", "metadata state file")
		placementGroups = fs.Uint64("placement-groups", 1024, "number of placement groups")
		replicas        = fs.Int("replicas", 3, "replication factor")
		coldAfter       = fs.Duration("cold-after", 30*24*time.Hour, "move objects to warm tier after this idle period")
		promoteOnRead   = fs.Bool("promote-on-read", true, "promote warm objects back to hot on read")
		scanInterval    = fs.Duration("tier-scan-interval", time.Minute, "tiering scan interval")
		rebalanceInt    = fs.Duration("rebalance-interval", 2*time.Minute, "rebalance scan interval")
	)
	fs.Parse(args)

	srv, err := gateway.New(gateway.Config{
		StatePath:         *statePath,
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
	ctx := signalContext()
	srv.RunBackground(ctx)
	httpSrv := &http.Server{Addr: *listen, Handler: srv.Handler()}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}()
	log.Printf("gateway listening on %s state=%s", *listen, *statePath)
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
  tierstore gateway -listen :9000 -state ./tierstore-state.json`)
}
