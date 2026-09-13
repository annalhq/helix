package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"helix/internal/kv"
)

func main() {
	id := flag.String("id", "kv-0", "node id")
	clientAddr := flag.String("client-addr", ":8000", "listen address for the JSON client edge")
	readMode := flag.String("read-mode", "local", "read path: local|log")
	flag.Parse()

	logger := log.New(os.Stderr, *id+" ", log.LstdFlags|log.Lmicroseconds|log.Lmsgprefix)

	mode, err := kv.ParseReadMode(*readMode)
	if err != nil {
		logger.Fatal(err)
	}

	ln, err := net.Listen("tcp", *clientAddr)
	if err != nil {
		logger.Fatal(err)
	}

	svc := kv.NewService(kv.NewStateMachine(), mode)
	edge := kv.NewEdge(svc, logger)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		logger.Printf("received %s, shutting down", <-sigs)
		ln.Close()
	}()

	logger.Printf("serving clients on %s (read-mode=%s)", ln.Addr(), svc.ReadMode())
	if err := edge.Serve(ln); err != nil {
		logger.Fatal(err)
	}
}
