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
func TestQueue_PriorityOrder(t *testing.T) {
	q := setupTestQueue(t)
	ctx := context.Background()

	// Add jobs with different priorities
	jobHigh, err := q.Add(ctx, "high", map[string]any{}, &oxmq.JobOptions{Priority: 0})
	if err != nil {
		t.Fatalf("failed to add high priority job: %v", err)
	}

	jobMedium, err := q.Add(ctx, "medium", map[string]any{}, &oxmq.JobOptions{Priority: 5})
	if err != nil {
		t.Fatalf("failed to add medium priority job: %v", err)
	}

	jobLow, err := q.Add(ctx, "low", map[string]any{}, &oxmq.JobOptions{Priority: 10})
	if err != nil {
		t.Fatalf("failed to add low priority job: %v", err)
	}

	// Fetch directly from the waiting ZSET to check score order
	jobIDs, err := q.Client().ZRange(ctx, q.KeyGen().Waiting(), 0, -1).Result()
	if err != nil {
		t.Fatalf("failed to fetch from waiting queue: %v", err)
	}

	// We expect high -> medium -> low
	expectedOrder := []string{jobHigh.ID, jobMedium.ID, jobLow.ID}
	if len(jobIDs) < 3 {
		t.Fatalf("expected at least 3 jobs in queue, got %d", len(jobIDs))
	}

	for i, want := range expectedOrder {
		if jobIDs[i] != want {
			t.Errorf("expected job %s at position %d, got %s", want, i, jobIDs[i])
		}
	}
}
