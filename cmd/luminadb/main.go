package main

import (
	"flag"
	"log"
	"strings"

	"github.com/user/luminadb/internal/raft"
	"github.com/user/luminadb/internal/storage"
	"github.com/user/luminadb/internal/transport"
	"github.com/user/luminadb/proto"
)

func main() {
	id := flag.String("id", "node1", "Node ID")
	addr := flag.String("addr", ":50051", "gRPC address")
	peersStr := flag.String("peers", "", "Comma-separated list of peer addresses")
	logDir := flag.String("logdir", "data", "Directory for storage logs")
	flag.Parse()

	var peers []string
	if *peersStr != "" {
		peers = strings.Split(*peersStr, ",")
	}

	// 1. Initialize Storage Engine
	engine, err := storage.NewEngine(*logDir)
	if err != nil {
		log.Fatalf("failed to create storage engine: %v", err)
	}
	defer engine.Close()

	// 2. Initialize Raft Node
	applyCh := make(chan *proto.LogEntry, 100)
	node := raft.NewNode(*id, peers, applyCh)

	// 3. Start State Machine Applier
	go func() {
		for entry := range applyCh {
			log.Printf("Applying entry: %s %s", entry.Type, entry.Key)
			switch entry.Type {
			case proto.LogEntry_PUT:
				engine.Put(entry.Key, entry.Value)
			case proto.LogEntry_DELETE:
				engine.Delete(entry.Key)
			}
		}
	}()

	// 4. Start Raft Node
	node.Start()

	// 5. Start gRPC Server
	server := transport.NewServer(node, engine)
	if err := server.Start(*addr); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
}
