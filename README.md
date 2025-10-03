[![Go Report Card](https://goreportcard.com/badge/github.com/jacobbailey8/oxmq)](https://goreportcard.com/report/github.com/jacobbailey8/oxmq)
[![Build Status](https://github.com/jacobbailey8/oxmq/actions/workflows/go.yml/badge.svg?branch=main)](https://github.com/jacobbailey8/oxmq/actions/workflows/go.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/jacobbailey8/oxmq.svg)](https://pkg.go.dev/github.com/jacobbailey8/oxmq)
[![License: MIT](https://img.shields.io/badge/License-MIT-db8851.svg)](https://opensource.org/licenses/MIT)
<!-- [![GitHub release](https://img.shields.io/github/v/release/jacobbailey8/oxmq)](https://github.com/jacobbailey8/oxmq/releases) -->

# OXMQ

**OXMQ** is a robust, Redis-backed job queue library for Go that provides reliable job processing with support for priorities, delays, retries, and concurrent workers. It is inspired by [BullMQ](https://github.com/taskforcesh/bullmq) in Node.js.

## Features

- **Priority-based processing** - Jobs with higher priority are processed first
- **Delayed jobs** - Schedule jobs to run at a future time
- **Automatic retries** - Configure retry behavior for failed jobs
- **Concurrent processing** - Control worker concurrency per queue
- **Job states** - Track jobs through waiting, active, completed, and failed states
- **Multiple workers** - Scale horizontally with multiple worker processes
- **Queue management** - Pause, resume, drain, and monitor queues
- **Custom job IDs** - Use your own identifiers for deduplication
- **Type-safe API** - Strongly-typed Go interfaces

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
- [Redis Configuration](#redis-configuration)
- [API Reference](#api-reference)
  - [Queue Operations](#queue-operations)
  - [Job Options](#job-options)
  - [Worker Configuration](#worker-configuration)
  - [Queue Management](#queue-management)
- [Usage Examples](#usage-examples)
- [Production Considerations](#production-considerations)
- [Best Practices](#best-practices)

## Installation

```bash
go get github.com/jacobbailey8/oxmq
```

**Requirements:**
- Go 1.25 or higher

## Quick Start

### Producer: Adding Jobs to a Queue

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/redis/go-redis/v9"
    "github.com/jacobbailey8/oxmq"
)

func main() {
    // Create Redis client
    rdb := redis.NewClient(&redis.Options{
        Addr:     "localhost:6379",
        DB:       0,  // use default DB
    })
    defer rdb.Close()

    // Create a queue
    queue := oxmq.NewQueue("email-queue", rdb)

    // Add a job with options
    job, err := queue.Add(context.Background(), "send-welcome-email", map[string]any{
        "to":      "user@example.com",
        "subject": "Welcome!",
        "body":    "Thanks for signing up",
    }, &oxmq.JobOptions{
        MaxRetries: 3,
        Priority:   5,
        Delay:      time.Minute * 5, // Process in 5 minutes
    })
    
    if err != nil {
        panic(err)
    }

    fmt.Printf("Job created: %s\n", job.ID)
}
```

### Consumer: Processing Jobs with a Worker

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "os/signal"
    "syscall"

    "github.com/redis/go-redis/v9"
    "github.com/jacobbailey8/oxmq"
)

func main() {
    // Create Redis client
    rdb := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })
    defer rdb.Close()

    // Create queue
    queue := oxmq.NewQueue("email-queue", rdb)

    // Define job processor
    processFn := func(job *oxmq.Job) (any, error) {
        to := job.Data["to"].(string)
        subject := job.Data["subject"].(string)
        
        log.Printf("Sending email to %s: %s", to, subject)
        
        // Your email sending logic here
        err := sendEmail(to, subject, job.Data["body"].(string))
        if err != nil {
            return nil, err // Job will be retried if retries are set
        }
        
        return map[string]any{
            "sent_at": time.Now(),
            "to":      to,
        }, nil
    }

    // Create worker
    worker, err := oxmq.NewWorker(queue, processFn, oxmq.WorkerConfig{
        Concurrency: 10, // Process up to 10 jobs concurrently
    })
    if err != nil {
        log.Fatal(err)
    }

    // Handle graceful shutdown
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

    go func() {
        <-sigChan
        log.Println("Shutting down worker...")
        cancel()
    }()

    // Start worker (blocks until context is cancelled)
    log.Println("Worker started")
    worker.Start(ctx)
    log.Println("Worker stopped")
}

func sendEmail(to, subject, body string) error {
    // Implementation here
    return nil
}
```

## Core Concepts

### Job States

Jobs flow through the following states:

```
┌─────────┐
│ Delayed │──(time elapsed)──┐
└─────────┘                  │
                             ▼
                        ┌─────────┐
                        │ Waiting │
                        └─────────┘
                             │
                             ▼
                        ┌────────┐
                        │ Active │
                        └────────┘
                             │
                    ┌────────┴────────┐
                    ▼                 ▼
              ┌───────────┐     ┌────────┐
              │ Completed │     │ Failed │
              └───────────┘     └────────┘
                                     │
                           (retry?)  │
                                     ▼
                             ┌─────────┐
                             │ Waiting │
                             └─────────┘
```

- **Waiting**: Job is queued and ready to be processed
- **Delayed**: Job is scheduled for future processing
- **Active**: Job is currently being processed by a worker
- **Completed**: Job finished successfully
- **Failed**: Job failed after exhausting all retries

### Priority System

Jobs are processed based on priority (lower values first). Jobs with the same priority are processed in FIFO order.

```go
// High priority job (processed first)
queue.Add(ctx, "critical-task", data, &oxmq.JobOptions{
    Priority: 0,
})

// Normal priority job
queue.Add(ctx, "normal-task", data, &oxmq.JobOptions{
    Priority: 5,
})

// Low priority job (processed last)
queue.Add(ctx, "background-task", data, &oxmq.JobOptions{
    Priority: 10,
})
```

### Retry Logic

When a job fails, OXMQ automatically retries it based on the `MaxRetries` option:

```go
job, err := queue.Add(ctx, "flaky-api-call", data, &oxmq.JobOptions{
    MaxRetries: 5, // Try up to 6 times total (initial + 5 retries)
})
```

Failed jobs are placed back in the waiting queue unless retries are exhausted.

## Redis Configuration

**OXMQ** uses [go-redis](https://github.com/redis/go-redis) as its Redis client. 

For more details on Redis configuration and options, see the [go-redis documentation](https://pkg.go.dev/github.com/redis/go-redis/v9).

## API Reference

### Queue Operations

#### Creating a Queue

```go
queue := oxmq.NewQueue(name string, client *redis.Client)
```

#### Creating a Job

```go
job, err := oxmq.NewJob("job-1", map[string]any{
        "to":      "user@example.com",
        "subject": "Welcome!",
        "body":    "Thanks for signing up",
    }, &oxmq.JobOptions{})
```

#### Adding Jobs

```go
// Add a single job
job, err := queue.Add(ctx context.Context, name string, data map[string]any, opts *JobOptions)

// Add an existing job
job, err := queue.AddJob(ctx context.Context, job *Job)

// Add multiple jobs concurrently
queue.AddBulk(ctx context.Context, jobs []*Job)
```

#### Retrieving Jobs

```go
// Get a specific job by ID
job, err := queue.GetJob(ctx context.Context, jobID string)

// Get jobs by state
jobs, err := queue.GetJobs(ctx context.Context, state JobState, start, stop int64)

// Convenience methods
jobs, err := queue.GetWaiting(ctx context.Context, start, stop int64)
jobs, err := queue.GetActive(ctx context.Context, start, stop int64)
jobs, err := queue.GetCompleted(ctx context.Context, start, stop int64)
jobs, err := queue.GetFailed(ctx context.Context, start, stop int64)
```

#### Queue Statistics

```go
// Get counts for all states
stats, err := queue.GetStats(ctx context.Context)
// stats.Waiting, stats.Active, stats.Completed, stats.Failed, stats.Delayed

// Count jobs in a specific state
count, err := queue.Count(ctx context.Context, state JobState)
```

### Job Options

```go
type JobOptions struct {
    MaxRetries int           // Number of retry attempts (default: 0)
    Delay      time.Duration // Delay before processing (default: 0)
    Priority   int           // Job priority, lower number = processed first (default: 0)
    CustomID   string        // Custom job identifier for deduplication
}
```

### Worker Configuration

```go
type WorkerConfig struct {
    Concurrency int // Number of concurrent jobs (default: 1)
}

worker, err := oxmq.NewWorker(queue, processFn, WorkerConfig{
    Concurrency: 20, // Process up to 20 jobs at once
})
```

### Queue Management

```go
// Pause queue (prevents new jobs from being processed)
err := queue.Pause(ctx)

// Resume queue
err := queue.Resume(ctx)

// Check if paused
paused, err := queue.IsPaused(ctx)

// Remove all waiting and delayed jobs
err := queue.Drain(ctx)

// Remove a specific job
err := queue.RemoveJob(ctx, jobID)

// Clean old jobs (remove completed/failed jobs older than 7 days)
count, err := queue.Clean(ctx, 7*24*time.Hour, oxmq.JobCompleted, 1000)

// Delete everything (all jobs and queue metadata)
err := queue.Obliterate(ctx)
```

## Usage Examples

### Scheduled Jobs

```go
// Schedule a reminder for 1 hour from now
queue.Add(ctx, "send-reminder", map[string]any{
    "user_id": 123,
    "message": "Your appointment is coming up",
}, &oxmq.JobOptions{
    Delay: time.Hour,
})
```

### Background Tasks with Different Priorities

```go
// Critical: user-facing operation
queue.Add(ctx, "process-payment", paymentData, &oxmq.JobOptions{
    Priority:   0,
    MaxRetries: 3,
})

// Normal: important but not urgent
queue.Add(ctx, "generate-invoice", invoiceData, &oxmq.JobOptions{
    Priority:   3,
    MaxRetries: 2,
})

// Low: background cleanup
queue.Add(ctx, "cleanup-temp-files", nil, &oxmq.JobOptions{
    Priority: 5,
})
```

### Bulk Job Creation

```go
jobs := make([]*oxmq.Job, 1000)
for i := range jobs {
    job, _ := oxmq.NewJob("process-record", map[string]any{
        "record_id": i,
    }, &oxmq.JobOptions{MaxRetries: 2})
    jobs[i] = job
}

// Add all jobs concurrently
queue.AddBulk(ctx, jobs)
```

### Job Deduplication with Custom IDs

```go
// Ensure only one welcome email per user
queue.Add(ctx, "send-welcome", map[string]any{
    "user_id": 456,
}, &oxmq.JobOptions{
    CustomID: fmt.Sprintf("welcome-email-%d", 456),
})

// Duplicate job with same CustomID will overwrite the previous one
```

### Multiple Workers on Different Machines

```go
// Machine 1
worker1, _ := oxmq.NewWorker(queue, processFn, oxmq.WorkerConfig{
    Concurrency: 10,
})
go worker1.Start(ctx)

// Machine 2
worker2, _ := oxmq.NewWorker(queue, processFn, oxmq.WorkerConfig{
    Concurrency: 10,
})
go worker2.Start(ctx)

// Both workers will process jobs from the same queue concurrently
```

### Monitoring Queue Health

```go
func monitorQueue(queue *oxmq.Queue) {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()
    
    for range ticker.C {
        stats, err := queue.GetStats(context.Background())
        if err != nil {
            log.Printf("Error getting stats: %v", err)
            continue
        }
        
        log.Printf("Queue stats - Waiting: %d, Active: %d, Completed: %d, Failed: %d",
            stats.Waiting, stats.Active, stats.Completed, stats.Failed)
        
        // Alert if too many failed jobs
        if stats.Failed > 100 {
            log.Printf("WARNING: High number of failed jobs: %d", stats.Failed)
        }
    }
}
```

## Production Considerations

### Graceful Shutdown

Always implement graceful shutdown to avoid job loss. Upon receiving the cancelation
signal, the worker will not take any more jobs off the queue. Any active jobs will continue 
being processed until finished.

```go
func main() {
    ctx, cancel := context.WithCancel(context.Background())
    
    // Capture shutdown signals
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
    
    go func() {
        <-sigChan
        log.Println("Shutdown signal received")
        cancel() // Cancel context to stop workers
    }()
    
    worker.Start(ctx)
    log.Println("Worker shutdown complete")
}
```

## Best Practices

1. **Keep job data serializable** - Use simple types (strings, numbers, bools, maps)
2. **Make jobs idempotent** - Jobs should be safe to retry
3. **Set appropriate retry limits** - Don't retry forever; failed jobs should eventually go to failed state

## License

MIT License - see LICENSE file for details

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.