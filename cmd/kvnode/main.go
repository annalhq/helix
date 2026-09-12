package main

import (
	"flag"
	"log"
)

func main() {
	id := flag.String("id", "kv-0", "node id")
	readMode := flag.String("read-mode", "local", "read path: local|log")
	flag.Parse()
	log.Printf("kvnode %s starting (read-mode=%s): not implemented yet", *id, *readMode)
}
