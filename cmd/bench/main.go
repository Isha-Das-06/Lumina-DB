package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/user/luminadb/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "Server address")
	concurrency := flag.Int("c", 10, "Concurrency level")
	requests := flag.Int("n", 1000, "Number of requests per worker")
	flag.Parse()

	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := proto.NewLuminaClient(conn)

	start := time.Now()
	var wg sync.WaitGroup

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < *requests; j++ {
				key := fmt.Sprintf("key-%d-%d", workerID, j)
				val := []byte(fmt.Sprintf("value-%d-%d", workerID, j))
				_, err := client.Put(context.Background(), &proto.PutRequest{Key: key, Value: val})
				if err != nil {
					// log.Printf("worker %d failed: %v", workerID, err)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	totalRequests := (*concurrency) * (*requests)
	fmt.Printf("Total Requests: %d\n", totalRequests)
	fmt.Printf("Total Time: %v\n", duration)
	fmt.Printf("Throughput: %.2f req/s\n", float64(totalRequests)/duration.Seconds())
}
 Lands
