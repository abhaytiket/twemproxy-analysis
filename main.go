package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	redisAddr   = "twemproxy-analysis-twemproxy:22121"
	totalKeys   = 1000000
	concurrency = 1000
)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	// Seed keys
	fmt.Println("Seeding keys...")
	for i := 1; i <= totalKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		err := rdb.Set(ctx, key, "value", 0).Err()
		if err != nil {
			log.Fatalf("Failed to set %s: %v", key, err)
		}
		if i%10000 == 0 {
			fmt.Printf("Seeded %d keys...\n", i)
		}
	}
	fmt.Println("Seeding complete.")

	// Add a delay of 60 seconds
	time.Sleep(60 * time.Second)
	fmt.Println("Starting deletion after 60 seconds delay...")

	// Concurrent DEL
	fmt.Println("Deleting keys concurrently...")
	wg := sync.WaitGroup{}
	keyCh := make(chan string, 1000)

	// Start workers
	for range concurrency {
		wg.Go(func() {
			for key := range keyCh {
				err := rdb.Del(ctx, key).Err()
				if err != nil {
					log.Printf("Failed to delete %s: %v", key, err)
				}
			}
		})
	}

	// Feed keys to channel
	for i := 1; i <= totalKeys; i++ {
		keyCh <- fmt.Sprintf("key-%d", i)
	}
	close(keyCh)

	wg.Wait()
	fmt.Println("All keys deleted.")
}
