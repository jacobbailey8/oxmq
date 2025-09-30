## Installation

Install **oxmq** via `go get`:

```bash
go get github.com/jacobbailey/oxmq
```

---

## Creating a Queue

**oxmq** uses [go-redis](https://github.com/redis/go-redis) as its Redis client.  
First, create a Redis client and then create a queue:

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/redis/go-redis/v9"
    "github.com/jacobbailey/oxmq/pkg/oxmq"
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
        Delay:      2 * time.Second,
        Priority:   1,
    })
    if err != nil {
        panic(err)
    }

    fmt.Printf("Created job with ID: %s\n", job.ID)
}
```

- `queue.Add` adds a new job with optional **JobOptions**.
- Supports delayed jobs, retries, and priorities out of the box.

For more details on Redis configuration and options, see the [go-redis documentation](https://pkg.go.dev/github.com/redis/go-redis/v9).
