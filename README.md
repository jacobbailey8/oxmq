# OXMQ

## Installation

Install **oxmq** via `go get`:

```bash
go get github.com/jacobbailey/oxmq
```

## Redis

**oxmq** uses [go-redis](https://github.com/redis/go-redis) as its Redis client. 

For more details on Redis configuration and options, see the [go-redis documentation](https://pkg.go.dev/github.com/redis/go-redis/v9).

## Creating a Queue

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/redis/go-redis/v9"
    "github.com/jacobbailey/oxmq"
)

func main() {
    // Create Redis client
    rdb := redis.NewClient(&redis.Options{
        Addr: "localhost:6379", // Redis server address
    })

    // Create a new queue
    queue := oxmq.NewQueue("my-queue", rdb)

    // Add a job to the queue
    job, err := queue.Add(context.Background(), "send-email", map[string]any{
        "to":      "user@example.com",
        "subject": "Hello from oxmq",
    }, &oxmq.JobOptions{
        MaxRetries: 3,
        Priority:   1,
    })
    if err != nil {
        panic(err)
    }

    fmt.Printf("Created job with ID: %s\n", job.ID)
}
```

## Creating a Worker

In another terminal or server, create a worker by connecting to the same redis
url and pass the worker the same queue name.

```go
package main

import (
	"context"
	"fmt"

	"github.com/jacobbailey8/oxmq"
	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379", // Same redis address as the queue
		DB:   15,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Make a queue of the same name you want to work on
	queue := oxmq.NewQueue("my-test-queue", rdb)

    // Define a function for the worker to execute upon receiving a job
    // function should return an error (or nil) and any date you'd like to store.
	process := func(job *oxmq.Job) (any, error) {
		fmt.Printf("processing job: %s\n", job.Name)
		return []int{1, 2, 3}, nil
	}

    // Define the worker
	worker, err := oxmq.NewWorker(queue, process, oxmq.WorkerConfig{Concurrency: 500000})

	if err != nil {
		fmt.Printf("Error creating worker: %s", err)
	}

	worker.Start(ctx)
}

```