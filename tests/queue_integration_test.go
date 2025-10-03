package tests

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jacobbailey8/oxmq"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

func setupTestQueue(t *testing.T) *oxmq.Queue {
	godotenv.Load()

	// Read environment variables
	redisAddr := os.Getenv("REDIS_ADDR")
	redisDB := os.Getenv("REDIS_DB")

	// Provide defaults if not set
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	if redisDB == "" {
		redisDB = "0"
	}

	// Convert REDIS_DB to int
	db, err := strconv.Atoi(redisDB)
	if err != nil {
		panic("Could not parse REDIS_DB: " + err.Error())
	}

	// Create Redis client
	client := redis.NewClient(&redis.Options{
		Addr: redisAddr, // Redis server address
		DB:   db,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Flush before each test
	if err := client.FlushDB(ctx).Err(); err != nil {
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
	if fetched.Data["to"] != "user@example.com" {
		t.Errorf("expected data to contain correct email")
	}
}

func TestQueue_PriorityOrder(t *testing.T) {
	q := setupTestQueue(t)
	ctx := context.Background()

	// Add jobs with different priorities (lower number = higher priority)
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

func TestQueue_DelayedJobs(t *testing.T) {
	q := setupTestQueue(t)
	ctx := context.Background()

	delay := 2 * time.Second
	job, err := q.Add(ctx, "delayed-job", map[string]any{}, &oxmq.JobOptions{Delay: delay})
	if err != nil {
		t.Fatalf("failed to add delayed job: %v", err)
	}

	// Job should be in delayed state
	fetched, err := q.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("failed to get job: %v", err)
	}

	if fetched.State != oxmq.JobDelayed {
		t.Errorf("expected job state to be delayed, got %s", fetched.State)
	}

	// Check delayed count
	count, err := q.Count(ctx, oxmq.JobDelayed)
	if err != nil {
		t.Fatalf("failed to count delayed jobs: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 delayed job, got %d", count)
	}
}

func TestQueue_AddBulk(t *testing.T) {
	q := setupTestQueue(t)
	ctx := context.Background()

	// Create 100 jobs
	jobs := make([]*oxmq.Job, 100)
	for i := range jobs {
		job, err := oxmq.NewJob("bulk-job", map[string]any{"index": i}, &oxmq.JobOptions{})
		if err != nil {
			t.Fatalf("failed to create job: %v", err)
		}
		jobs[i] = job
	}

	// Add all jobs in bulk
	q.AddBulk(ctx, jobs)

	// Verify all jobs were added
	count, err := q.Count(ctx, oxmq.JobWaiting)
	if err != nil {
		t.Fatalf("failed to count waiting jobs: %v", err)
	}

	if count != 100 {
		t.Errorf("expected 100 jobs, got %d", count)
	}

	// Also verify we can retrieve them
	allJobs, err := q.GetWaiting(ctx, -1, -1)
	if err != nil {
		t.Fatalf("failed to get waiting jobs: %v", err)
	}
	if len(allJobs) != 100 {
		t.Errorf("expected to retrieve 100 jobs, got %d", len(allJobs))
	}
}

func TestQueue_GetStats(t *testing.T) {
	q := setupTestQueue(t)
	ctx := context.Background()

	// Add jobs to different states
	q.Add(ctx, "job1", map[string]any{}, &oxmq.JobOptions{})
	q.Add(ctx, "job2", map[string]any{}, &oxmq.JobOptions{})
	q.Add(ctx, "job3", map[string]any{}, &oxmq.JobOptions{Delay: time.Second})

	stats, err := q.GetStats(ctx)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}

	if stats.Waiting != 2 {
		t.Errorf("expected 2 waiting jobs, got %d", stats.Waiting)
	}
	if stats.Delayed != 1 {
		t.Errorf("expected 1 delayed job, got %d", stats.Delayed)
	}
	if stats.Active != 0 {
		t.Errorf("expected 0 active jobs, got %d", stats.Active)
	}
}

func TestQueue_PlaceJobInActive(t *testing.T) {
	q := setupTestQueue(t)
	ctx := context.Background()

	// Add a job
	job, err := q.Add(ctx, "test-job", map[string]any{}, &oxmq.JobOptions{})
	if err != nil {
		t.Fatalf("failed to add job: %v", err)
	}

	// Move to active
	activeJob, err := q.PlaceJobInActive(ctx, job.ID)
	if err != nil {
		t.Fatalf("failed to place job in active: %v", err)
	}

	if activeJob.State != oxmq.JobActive {
		t.Errorf("expected job state to be active, got %s", activeJob.State)
	}

	// Verify counts
	waitingCount, _ := q.Count(ctx, oxmq.JobWaiting)
	activeCount, _ := q.Count(ctx, oxmq.JobActive)

	if waitingCount != 0 {
		t.Errorf("expected 0 waiting jobs, got %d", waitingCount)
	}
	if activeCount != 1 {
		t.Errorf("expected 1 active job, got %d", activeCount)
	}
}

func TestQueue_PlaceJobInCompleted(t *testing.T) {
	q := setupTestQueue(t)
	ctx := context.Background()

	job, err := q.Add(ctx, "test-job", map[string]any{}, &oxmq.JobOptions{})
	if err != nil {
		t.Fatalf("failed to add job: %v", err)
	}

	// Move to active first
	activeJob, err := q.PlaceJobInActive(ctx, job.ID)
	if err != nil {
		t.Fatalf("failed to place job in active: %v", err)
	}

	// Remove from active and mark completed
	err = q.RemoveJobFromActive(ctx, activeJob)
	if err != nil {
		t.Fatalf("failed to remove job from active: %v", err)
	}

	returnData := map[string]any{"result": "success"}
	err = q.PlaceJobInCompleted(ctx, activeJob, returnData)
	if err != nil {
		t.Fatalf("failed to place job in completed: %v", err)
	}

	// Verify state
	completed, err := q.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("failed to get completed job: %v", err)
	}

	if completed.State != oxmq.JobCompleted {
		t.Errorf("expected job state to be completed, got %s", completed.State)
	}
	if completed.ReturnValue == nil {
		t.Error("expected return value to be set")
	}
}

func TestQueue_PlaceJobInFailed(t *testing.T) {
	q := setupTestQueue(t)
	ctx := context.Background()

	job, err := q.Add(ctx, "test-job", map[string]any{}, &oxmq.JobOptions{})
	if err != nil {
		t.Fatalf("failed to add job: %v", err)
	}

	// Move to active
	activeJob, err := q.PlaceJobInActive(ctx, job.ID)
	if err != nil {
		t.Fatalf("failed to place job in active: %v", err)
	}

	// Remove from active
	err = q.RemoveJobFromActive(ctx, activeJob)
	if err != nil {
		t.Fatalf("failed to remove job from active: %v", err)
	}

	// Mark as failed
	activeJob.MarkFailed(fmt.Errorf("something went wrong"))
	err = q.PlaceJobInFailed(ctx, activeJob)
	if err != nil {
		t.Fatalf("failed to place job in failed: %v", err)
	}

	// Verify state
	failed, err := q.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("failed to get failed job: %v", err)
	}

	if failed.State != oxmq.JobFailed {
		t.Errorf("expected job state to be failed, got %s", failed.State)
	}
}

func TestQueue_GetJobs(t *testing.T) {
	q := setupTestQueue(t)
	ctx := context.Background()

	// Add multiple jobs
	for i := 0; i < 5; i++ {
		q.Add(ctx, "test-job", map[string]any{"num": i}, &oxmq.JobOptions{})
	}

	// Get first 3 jobs
	jobs, err := q.GetWaiting(ctx, 0, 2)
	if err != nil {
		t.Fatalf("failed to get waiting jobs: %v", err)
	}

	if len(jobs) != 3 {
		t.Errorf("expected 3 jobs, got %d", len(jobs))
	}

	// Get all jobs
	allJobs, err := q.GetWaiting(ctx, -1, -1)
	if err != nil {
		t.Fatalf("failed to get all waiting jobs: %v", err)
	}

	if len(allJobs) != 5 {
		t.Errorf("expected 5 jobs, got %d", len(allJobs))
	}
}

func TestQueue_RemoveJob(t *testing.T) {
	q := setupTestQueue(t)
	ctx := context.Background()

	job, err := q.Add(ctx, "test-job", map[string]any{}, &oxmq.JobOptions{})
	if err != nil {
		t.Fatalf("failed to add job: %v", err)
	}

	// Remove the job
	err = q.RemoveJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("failed to remove job: %v", err)
	}

	// Verify it's gone
	_, err = q.GetJob(ctx, job.ID)
	if err == nil {
		t.Error("expected error when getting removed job")
	}

	count, _ := q.Count(ctx, oxmq.JobWaiting)
	if count != 0 {
		t.Errorf("expected 0 jobs, got %d", count)
	}
}

func TestQueue_Clean(t *testing.T) {
	q := setupTestQueue(t)
	ctx := context.Background()

	// Add a job and complete it
	job, err := q.Add(ctx, "old-job", map[string]any{}, &oxmq.JobOptions{})
	if err != nil {
		t.Fatalf("failed to add job: %v", err)
	}

	activeJob, _ := q.PlaceJobInActive(ctx, job.ID)
	q.RemoveJobFromActive(ctx, activeJob)
	q.PlaceJobInCompleted(ctx, activeJob, nil)

	// Manually set UpdatedAt to past (simulate old job)
	time.Sleep(100 * time.Millisecond)

	// Clean jobs older than 50ms
	cleaned, err := q.Clean(ctx, 50*time.Millisecond, oxmq.JobCompleted, 100)
	if err != nil {
		t.Fatalf("failed to clean jobs: %v", err)
	}

	if cleaned != 1 {
		t.Errorf("expected 1 cleaned job, got %d", cleaned)
	}
}

func TestQueue_PauseAndResume(t *testing.T) {
	q := setupTestQueue(t)
	ctx := context.Background()

	// Initially not paused
	paused, err := q.IsPaused(ctx)
	if err != nil {
		t.Fatalf("failed to check pause state: %v", err)
	}
	if paused {
		t.Error("expected queue to not be paused initially")
	}

	// Pause the queue
	err = q.Pause(ctx)
	if err != nil {
		t.Fatalf("failed to pause queue: %v", err)
	}

	paused, err = q.IsPaused(ctx)
	if err != nil {
		t.Fatalf("failed to check pause state: %v", err)
	}
	if !paused {
		t.Error("expected queue to be paused")
	}

	// Resume the queue
	err = q.Resume(ctx)
	if err != nil {
		t.Fatalf("failed to resume queue: %v", err)
	}

	paused, err = q.IsPaused(ctx)
	if err != nil {
		t.Fatalf("failed to check pause state: %v", err)
	}
	if paused {
		t.Error("expected queue to not be paused after resume")
	}
}

func TestQueue_Drain(t *testing.T) {
	q := setupTestQueue(t)
	ctx := context.Background()

	// Add waiting and delayed jobs
	q.Add(ctx, "waiting-job", map[string]any{}, &oxmq.JobOptions{})
	q.Add(ctx, "delayed-job", map[string]any{}, &oxmq.JobOptions{Delay: time.Second})

	// Add an active job (should not be drained)
	job, _ := q.Add(ctx, "active-job", map[string]any{}, &oxmq.JobOptions{})
	q.PlaceJobInActive(ctx, job.ID)

	// Drain the queue
	err := q.Drain(ctx)
	if err != nil {
		t.Fatalf("failed to drain queue: %v", err)
	}

	// Verify waiting and delayed are empty
	waitingCount, _ := q.Count(ctx, oxmq.JobWaiting)
	delayedCount, _ := q.Count(ctx, oxmq.JobDelayed)
	activeCount, _ := q.Count(ctx, oxmq.JobActive)

	if waitingCount != 0 {
		t.Errorf("expected 0 waiting jobs after drain, got %d", waitingCount)
	}
	if delayedCount != 0 {
		t.Errorf("expected 0 delayed jobs after drain, got %d", delayedCount)
	}
	if activeCount != 1 {
		t.Errorf("expected 1 active job (not drained), got %d", activeCount)
	}
}

func TestQueue_Obliterate(t *testing.T) {
	q := setupTestQueue(t)
	ctx := context.Background()

	// Add jobs in various states
	q.Add(ctx, "job1", map[string]any{}, &oxmq.JobOptions{})
	q.Add(ctx, "job2", map[string]any{}, &oxmq.JobOptions{Delay: time.Second})
	job, _ := q.Add(ctx, "job3", map[string]any{}, &oxmq.JobOptions{})
	q.PlaceJobInActive(ctx, job.ID)

	// Obliterate everything
	err := q.Obliterate(ctx)
	if err != nil {
		t.Fatalf("failed to obliterate queue: %v", err)
	}

	// Verify all states are empty
	stats, err := q.GetStats(ctx)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}

	if stats.Waiting != 0 || stats.Delayed != 0 || stats.Active != 0 {
		t.Errorf("expected all counts to be 0, got waiting=%d, delayed=%d, active=%d",
			stats.Waiting, stats.Delayed, stats.Active)
	}
}

func TestQueue_JobNotFound(t *testing.T) {
	q := setupTestQueue(t)
	ctx := context.Background()

	_, err := q.GetJob(ctx, "non-existent-id")
	if err != oxmq.ErrJobNotFound {
		t.Errorf("expected ErrJobNotFound, got %v", err)
	}
}

func TestQueue_PlaceJobInWaiting(t *testing.T) {
	q := setupTestQueue(t)
	ctx := context.Background()

	// Create and add a job
	job, err := q.Add(ctx, "test-job", map[string]any{}, &oxmq.JobOptions{Priority: 5})
	if err != nil {
		t.Fatalf("failed to add job: %v", err)
	}

	// Move to active
	activeJob, err := q.PlaceJobInActive(ctx, job.ID)
	if err != nil {
		t.Fatalf("failed to place job in active: %v", err)
	}

	// Remove from active
	err = q.RemoveJobFromActive(ctx, activeJob)
	if err != nil {
		t.Fatalf("failed to remove from active: %v", err)
	}

	// Place back in waiting (retry scenario)
	err = q.PlaceJobInWaiting(ctx, activeJob)
	if err != nil {
		t.Fatalf("failed to place job in waiting: %v", err)
	}

	// Verify state
	fetched, err := q.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("failed to get job: %v", err)
	}

	if fetched.State != oxmq.JobWaiting {
		t.Errorf("expected job state to be waiting, got %s", fetched.State)
	}
}

func TestQueue_MultipleQueues(t *testing.T) {
	godotenv.Load()

	// Read environment variables
	redisAddr := os.Getenv("REDIS_ADDR")
	redisDB := os.Getenv("REDIS_DB")

	// Provide defaults if not set
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	if redisDB == "" {
		redisDB = "0"
	}

	// Convert REDIS_DB to int
	db, err := strconv.Atoi(redisDB)
	if err != nil {
		panic("Could not parse REDIS_DB: " + err.Error())
	}

	// Create Redis client
	client := redis.NewClient(&redis.Options{
		Addr: redisAddr, // Redis server address
		DB:   db,
	})
	ctx := context.Background()
	client.FlushDB(ctx)

	q1 := oxmq.NewQueue("queue-1", client)
	q2 := oxmq.NewQueue("queue-2", client)

	// Add jobs to different queues
	q1.Add(ctx, "job1", map[string]any{}, &oxmq.JobOptions{})
	q2.Add(ctx, "job2", map[string]any{}, &oxmq.JobOptions{})

	// Verify isolation
	count1, _ := q1.Count(ctx, oxmq.JobWaiting)
	count2, _ := q2.Count(ctx, oxmq.JobWaiting)

	if count1 != 1 {
		t.Errorf("expected queue-1 to have 1 job, got %d", count1)
	}
	if count2 != 1 {
		t.Errorf("expected queue-2 to have 1 job, got %d", count2)
	}
}

func TestQueue_CustomID(t *testing.T) {
	q := setupTestQueue(t)
	ctx := context.Background()

	customID := "my-custom-id-123"
	job, err := q.Add(ctx, "custom-job", map[string]any{"data": "test"}, &oxmq.JobOptions{
		CustomID: customID,
	})
	if err != nil {
		t.Fatalf("failed to add job with custom ID: %v", err)
	}

	if job.ID != customID {
		t.Errorf("expected job ID to be %s, got %s", customID, job.ID)
	}
	if job.CustomID != customID {
		t.Errorf("expected custom ID to be %s, got %s", customID, job.CustomID)
	}

	// Verify we can fetch it by custom ID
	fetched, err := q.GetJob(ctx, customID)
	if err != nil {
		t.Fatalf("failed to get job by custom ID: %v", err)
	}

	if fetched.Name != "custom-job" {
		t.Errorf("expected name custom-job, got %s", fetched.Name)
	}
}

func TestQueue_DedupDrop(t *testing.T) {
	q := setupTestQueue(t)
	ctx := context.Background()

	customID := "dedup-drop-test"

	// Add first job with dedup=drop
	job1, err := q.Add(ctx, "test-job", map[string]any{"version": 1}, &oxmq.JobOptions{
		CustomID: customID,
		Dedup:    oxmq.DedupDrop,
	})
	if err != nil {
		t.Fatalf("failed to add first job: %v", err)
	}

	// Try to add second job with same custom ID
	job2, err := q.Add(ctx, "test-job", map[string]any{"version": 2}, &oxmq.JobOptions{
		CustomID: customID,
		Dedup:    oxmq.DedupDrop,
	})
	if err != nil {
		t.Fatalf("failed to add second job: %v", err)
	}

	// job2 should be the same as job1 (original returned)
	if job2.ID != job1.ID {
		t.Errorf("expected same job ID, got %s and %s", job1.ID, job2.ID)
	}

	// Verify data is still from first job
	fetched, err := q.GetJob(ctx, customID)
	if err != nil {
		t.Fatalf("failed to get job: %v", err)
	}

	if fetched.Data["version"] != float64(1) { // JSON unmarshals numbers as float64
		t.Errorf("expected version 1 (original), got %v", fetched.Data["version"])
	}

	// Should only have one job in waiting
	count, _ := q.Count(ctx, oxmq.JobWaiting)
	if count != 1 {
		t.Errorf("expected 1 job in waiting, got %d", count)
	}
}

func TestQueue_DedupAdd(t *testing.T) {
	q := setupTestQueue(t)
	ctx := context.Background()

	customID := "dedup-add-test"

	// Add first job with dedup=add
	job1, err := q.Add(ctx, "test-job", map[string]any{"version": 1}, &oxmq.JobOptions{
		CustomID: customID,
		Dedup:    oxmq.DedupAdd,
	})
	if err != nil {
		t.Fatalf("failed to add first job: %v", err)
	}

	// Try to add second job with same custom ID and dedup=add
	job2, err := q.Add(ctx, "test-job", map[string]any{"version": 2}, &oxmq.JobOptions{
		CustomID: customID,
		Dedup:    oxmq.DedupAdd,
	})
	if err != nil {
		t.Fatalf("failed to add second job: %v", err)
	}

	// job2 should have a different ID (new one generated)
	if job2.ID == job1.ID {
		t.Errorf("expected different job IDs, both are %s", job1.ID)
	}

	// Both jobs should exist
	_, err = q.GetJob(ctx, job1.ID)
	if err != nil {
		t.Errorf("first job should still exist: %v", err)
	}

	_, err = q.GetJob(ctx, job2.ID)
	if err != nil {
		t.Errorf("second job should exist: %v", err)
	}

	// Should have two jobs in waiting
	count, _ := q.Count(ctx, oxmq.JobWaiting)
	if count != 2 {
		t.Errorf("expected 2 jobs in waiting, got %d", count)
	}
}
func TestQueue_DedupUpdate(t *testing.T) {
	q := setupTestQueue(t)
	ctx := context.Background()

	customID := "dedup-update-test"

	// Add first job with dedup=update (default)
	job1, err := q.Add(ctx, "test-job", map[string]any{"version": 1}, &oxmq.JobOptions{
		CustomID: customID,
		Dedup:    oxmq.DedupUpdate,
		Priority: 5,
	})
	if err != nil {
		t.Fatalf("failed to add first job: %v", err)
	}

	// Add second job with same custom ID - should update
	job2, err := q.Add(ctx, "test-job", map[string]any{"version": 2}, &oxmq.JobOptions{
		CustomID: customID,
		Dedup:    oxmq.DedupUpdate,
		Priority: 10,
	})
	if err != nil {
		t.Fatalf("failed to add second job: %v", err)
	}

	// IDs should be the same
	if job2.ID != job1.ID {
		t.Errorf("expected same job ID, got %s and %s", job1.ID, job2.ID)
	}

	// Verify data is updated
	fetched, err := q.GetJob(ctx, customID)
	if err != nil {
		t.Fatalf("failed to get job: %v", err)
	}

	if fetched.Data["version"] != float64(2) {
		t.Errorf("expected version 2 (updated), got %v", fetched.Data["version"])
	}

	if fetched.Priority != 10 {
		t.Errorf("expected priority 10, got %d", fetched.Priority)
	}

	// Should only have one job in waiting
	count, _ := q.Count(ctx, oxmq.JobWaiting)
	if count != 1 {
		t.Errorf("expected 1 job in waiting, got %d", count)
	}
}

func TestQueue_DedupWithoutCustomID(t *testing.T) {
	q := setupTestQueue(t)
	ctx := context.Background()

	// Add jobs without custom ID - dedup should not apply
	job1, err := q.Add(ctx, "test-job", map[string]any{"data": "first"}, &oxmq.JobOptions{
		Dedup: oxmq.DedupDrop,
	})
	if err != nil {
		t.Fatalf("failed to add first job: %v", err)
	}

	job2, err := q.Add(ctx, "test-job", map[string]any{"data": "second"}, &oxmq.JobOptions{
		Dedup: oxmq.DedupDrop,
	})
	if err != nil {
		t.Fatalf("failed to add second job: %v", err)
	}

	// Should have different IDs since no custom ID
	if job1.ID == job2.ID {
		t.Errorf("expected different IDs without custom ID, both are %s", job1.ID)
	}

	// Should have two jobs
	count, _ := q.Count(ctx, oxmq.JobWaiting)
	if count != 2 {
		t.Errorf("expected 2 jobs without custom ID dedup, got %d", count)
	}
}

func TestQueue_DedupWithDelayedJobs(t *testing.T) {
	q := setupTestQueue(t)
	ctx := context.Background()

	customID := "delayed-dedup-test"

	// Add delayed job
	job1, err := q.Add(ctx, "test-job", map[string]any{"version": 1}, &oxmq.JobOptions{
		CustomID: customID,
		Delay:    2 * time.Second,
		Dedup:    oxmq.DedupDrop,
	})
	if err != nil {
		t.Fatalf("failed to add delayed job: %v", err)
	}

	if job1.State != oxmq.JobDelayed {
		t.Errorf("expected job to be delayed, got %s", job1.State)
	}

	// Try to add another with same custom ID
	job2, err := q.Add(ctx, "test-job", map[string]any{"version": 2}, &oxmq.JobOptions{
		CustomID: customID,
		Dedup:    oxmq.DedupDrop,
	})
	if err != nil {
		t.Fatalf("failed to add second job: %v", err)
	}

	// Should return original delayed job
	if job2.ID != job1.ID {
		t.Errorf("expected same job ID, got %s and %s", job1.ID, job2.ID)
	}

	// Should only have one delayed job
	count, _ := q.Count(ctx, oxmq.JobDelayed)
	if count != 1 {
		t.Errorf("expected 1 delayed job, got %d", count)
	}
}
