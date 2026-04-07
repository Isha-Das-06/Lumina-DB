package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/user/luminadb/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "Server address")
	flag.Parse()
	args := flag.Args()

	if len(args) < 1 {
		fmt.Println("Usage: lumina-cli [-addr host:port] <put|get|delete> [args]")
		fmt.Println("Example: lumina-cli put user:1 Alice")
		return
	}

	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock(), grpc.WithTimeout(2*time.Second))
	if err != nil {
		log.Fatalf("failed to connect to %s: %v", *addr, err)
	}
	defer conn.Close()
	client := proto.NewLuminaClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch args[0] {
	case "put":
		if len(args) < 3 {
			fmt.Println("Usage: put <key> <value>")
			return
		}
		resp, err := client.Put(ctx, &proto.PutRequest{
			Key:   args[1],
			Value: []byte(args[2]),
		})
		if err != nil {
			log.Fatalf("RPC error: %v", err)
		}
		if resp.Success {
			fmt.Println("OK")
		} else {
			fmt.Printf("Failed. Current Leader: %s\n", resp.LeaderId)
		}

	case "get":
		if len(args) < 2 {
			fmt.Println("Usage: get <key>")
			return
		}
		resp, err := client.Get(ctx, &proto.GetRequest{Key: args[1]})
		if err != nil {
			log.Fatalf("RPC error: %v", err)
		}
		if resp.Found {
			fmt.Printf("%s\n", string(resp.Value))
		} else {
			fmt.Println("Key not found")
		}

	case "delete":
		if len(args) < 2 {
			fmt.Println("Usage: delete <key>")
			return
		}
		resp, err := client.Delete(ctx, &proto.DeleteRequest{Key: args[1]})
		if err != nil {
			log.Fatalf("RPC error: %v", err)
		}
		if resp.Success {
			fmt.Println("OK")
		} else {
			fmt.Println("Failed")
		}

	default:
		fmt.Printf("Unknown command: %s\n", args[0])
	}
}
