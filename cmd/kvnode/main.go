package main

import (
	"flag"
	"fmt"
	"log"
	"maps"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	"helix/internal/kv"
	"helix/internal/raft"
	"helix/internal/rpcpeer"
)

func main() {
	id := flag.String("id", "kv-0", "node id")
	clientAddr := flag.String("client-addr", ":8000", "listen address for the JSON client edge")
	raftAddr := flag.String("raft-addr", ":7000", "listen address for internal net/rpc (raft and forwarding)")
	peersFlag := flag.String("peers", "", "comma-separated id=host:port raft addresses of cluster members (self is ignored)")
	dataDir := flag.String("data-dir", "data", "directory for the raft write-ahead log")
	readMode := flag.String("read-mode", "local", "read path: local|log")
	flag.Parse()

	logger := log.New(os.Stderr, *id+" ", log.LstdFlags|log.Lmicroseconds|log.Lmsgprefix)

	mode, err := kv.ParseReadMode(*readMode)
	if err != nil {
		logger.Fatal(err)
	}
	peers, err := parsePeers(*peersFlag, *id)
	if err != nil {
		logger.Fatal(err)
	}
	storage, err := raft.OpenFileStorage(filepath.Join(*dataDir, "raft.wal"))
	if err != nil {
		logger.Fatal(err)
	}
	defer storage.Close()

	svc := kv.NewService(kv.NewStateMachine(), mode)
	node, err := raft.New(raft.Config{
		ID:        *id,
		Peers:     slices.Sorted(maps.Keys(peers)),
		Transport: raft.NewRPCTransport(rpcpeer.New(peers)),
		Storage:   storage,
		Logger:    logger,
	}, svc.Apply)
	if err != nil {
		logger.Fatal(err)
	}
	svc.Attach(node, kv.NewRPCForwarder(rpcpeer.New(peers)))

	rpcSrv := rpc.NewServer()
	if err := rpcSrv.RegisterName("Raft", raft.NewRPCServer(node)); err != nil {
		logger.Fatal(err)
	}
	if err := rpcSrv.RegisterName("KV", kv.NewForwardServer(svc)); err != nil {
		logger.Fatal(err)
	}

	raftLn, err := net.Listen("tcp", *raftAddr)
	if err != nil {
		logger.Fatal(err)
	}
	clientLn, err := net.Listen("tcp", *clientAddr)
	if err != nil {
		logger.Fatal(err)
	}

	node.Start()
	go func() {
		if err := rpcpeer.Serve(raftLn, rpcSrv); err != nil {
			logger.Fatal(err)
		}
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		logger.Printf("received %s, shutting down", <-sigs)
		clientLn.Close()
		raftLn.Close()
	}()

	logger.Printf("serving clients on %s, raft on %s, peers %v (read-mode=%s)", clientLn.Addr(), raftLn.Addr(), peers, svc.ReadMode())
	if err := kv.NewEdge(svc, logger).Serve(clientLn); err != nil {
		logger.Fatal(err)
	}
	node.Stop()
}

func parsePeers(s, self string) (map[string]string, error) {
	peers := make(map[string]string)
	if strings.TrimSpace(s) == "" {
		return peers, nil
	}
	for _, part := range strings.Split(s, ",") {
		id, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("invalid peer %q (want id=host:port)", part)
		}
		if id != self {
			peers[id] = addr
		}
	}
	return peers, nil
}
