package tests

import (
	"context"
	"testing"

	"github.com/jacobbailey8/oxmq"
	"github.com/redis/go-redis/v9"
)

func setupTestQueue(t *testing.T) *oxmq.Queue {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15, // test DB
	})

	// Flush before each test
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("failed to flush Redis: %v", err)
	}

	return oxmq.NewQueue("test-queue", client)
}

func TestQueue_AddAndGetJob(t *testing.T) {
	q := setupTestQueue(t)
	ctx := context.Background()

	job, err := q.Add(ctx, "email", map[string]any{"to": "user@example.com"}, &oxmq.JobOptions{})
	if err != nil {
		t.Fatalf("failed to add job: %v", err)
	}

	fetched, err := q.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("failed to get job: %v", err)
	}

	if fetched.Name != "email" {
		t.Errorf("expected name %s, got %s", "email", fetched.Name)
	}
}
