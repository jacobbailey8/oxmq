package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jacobbailey8/oxmq"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test helper to setup Redis client and queue
func setupTestWorkerQueue(t *testing.T) (*redis.Client, *oxmq.Queue, func()) {
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
	err = client.Ping(ctx).Err()
	require.NoError(t, err, "Redis must be running for integration tests")

	queueName := fmt.Sprintf("test-queue-%d", time.Now().UnixNano())
	queue := oxmq.NewQueue(queueName, client)

	cleanup := func() {
		queue.Obliterate(ctx)
		client.Close()
	}

	return client, queue, cleanup
}

// TestWorkerBasicProcessing tests that a worker can process a simple job
func TestWorkerBasicProcessing(t *testing.T) {
	_, queue, cleanup := setupTestWorkerQueue(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	processed := make(chan string, 1)

	processFn := func(job *oxmq.Job) (any, error) {
		processed <- job.ID
		return "success", nil
	}

	worker, err := oxmq.NewWorker(queue, processFn, oxmq.WorkerConfig{Concurrency: 1})
	require.NoError(t, err)

	// Start worker in background
	go worker.Start(ctx)

	// Add a job
	job, err := queue.Add(context.Background(), "test-job", map[string]any{"key": "value"}, nil)
	require.NoError(t, err)

	// Wait for processing
	select {
	case processedID := <-processed:
		assert.Equal(t, job.ID, processedID)
	case <-time.After(3 * time.Second):
		t.Fatal("Job was not processed in time")
	}

	// Verify job is in completed state
	time.Sleep(100 * time.Millisecond) // Give time for state update
	completedJobs, err := queue.GetCompleted(context.Background(), 0, -1)
	require.NoError(t, err)
	assert.Len(t, completedJobs, 1)
	assert.Equal(t, job.ID, completedJobs[0].ID)
	assert.Equal(t, "success", completedJobs[0].ReturnValue)
}

// TestWorkerConcurrentProcessing tests multiple jobs being processed concurrently
func TestWorkerConcurrentProcessing(t *testing.T) {
	_, queue, cleanup := setupTestWorkerQueue(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const numJobs = 10
	const concurrency = 5

	var processedCount atomic.Int32
	var mu sync.Mutex
	processedJobs := make(map[string]bool)

	processFn := func(job *oxmq.Job) (any, error) {
		time.Sleep(100 * time.Millisecond) // Simulate work
		mu.Lock()
		processedJobs[job.ID] = true
		mu.Unlock()
		processedCount.Add(1)
		return nil, nil
	}

	worker, err := oxmq.NewWorker(queue, processFn, oxmq.WorkerConfig{Concurrency: concurrency})
	require.NoError(t, err)

	go worker.Start(ctx)

	// Add multiple jobs
	jobIDs := make([]string, numJobs)
	for i := 0; i < numJobs; i++ {
		job, err := queue.Add(context.Background(), "test-job", map[string]any{"index": i}, nil)
		require.NoError(t, err)
		jobIDs[i] = job.ID
	}

	// Wait for all jobs to be processed
	deadline := time.After(8 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatalf("Not all jobs processed in time. Processed: %d/%d", processedCount.Load(), numJobs)
		case <-ticker.C:
			if processedCount.Load() == numJobs {
				goto DONE
			}
		}
	}

DONE:
	// Verify all jobs were processed
	mu.Lock()
	assert.Len(t, processedJobs, numJobs)
	mu.Unlock()

	// Verify all jobs are completed
	time.Sleep(200 * time.Millisecond)
	stats, err := queue.GetStats(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(numJobs), stats.Completed)
	assert.Equal(t, int64(0), stats.Active)
}

// TestWorkerJobRetry tests that failed jobs are retried
func TestWorkerJobRetry(t *testing.T) {
	_, queue, cleanup := setupTestWorkerQueue(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var attemptCount atomic.Int32
	processed := make(chan bool, 1)

	processFn := func(job *oxmq.Job) (any, error) {
		count := attemptCount.Add(1)
		if count < 3 {
			return nil, errors.New("simulated failure")
		}
		processed <- true
		return "success on retry", nil
	}

	worker, err := oxmq.NewWorker(queue, processFn, oxmq.WorkerConfig{Concurrency: 1})
	require.NoError(t, err)

	go worker.Start(ctx)

	// Add job with retries
	job, err := queue.Add(context.Background(), "retry-job", map[string]any{}, &oxmq.JobOptions{
		MaxRetries: 3,
	})
	require.NoError(t, err)

	// Wait for successful processing
	select {
	case <-processed:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("Job was not successfully processed after retries")
	}

	assert.Equal(t, int32(3), attemptCount.Load())

	// Verify job is completed
	time.Sleep(200 * time.Millisecond)
	completedJobs, err := queue.GetCompleted(context.Background(), 0, -1)
	require.NoError(t, err)
	require.Len(t, completedJobs, 1)
	assert.Equal(t, job.ID, completedJobs[0].ID)
	assert.Equal(t, 3, completedJobs[0].Attempts) // 2 failed attempts before success
}

// TestWorkerJobFailure tests that jobs that exceed retries are marked as failed
func TestWorkerJobFailure(t *testing.T) {
	_, queue, cleanup := setupTestWorkerQueue(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var attemptCount atomic.Int32
	expectedError := errors.New("permanent failure")

	processFn := func(job *oxmq.Job) (any, error) {
		attemptCount.Add(1)
		return nil, expectedError
	}

	worker, err := oxmq.NewWorker(queue, processFn, oxmq.WorkerConfig{Concurrency: 1})
	require.NoError(t, err)

	go worker.Start(ctx)

	// Add job with limited retries
	job, err := queue.Add(context.Background(), "failing-job", map[string]any{}, &oxmq.JobOptions{
		MaxRetries: 2,
	})
	require.NoError(t, err)

	// Wait for all attempts to complete
	time.Sleep(2 * time.Second)

	assert.Equal(t, int32(3), attemptCount.Load()) // Initial + 2 retries

	// Verify job is in failed state
	failedJobs, err := queue.GetFailed(context.Background(), 0, -1)
	require.NoError(t, err)
	require.Len(t, failedJobs, 1)
	assert.Equal(t, job.ID, failedJobs[0].ID)
	assert.Equal(t, oxmq.JobFailed, failedJobs[0].State)
	assert.Contains(t, failedJobs[0].Error, expectedError.Error())
	assert.Equal(t, 3, failedJobs[0].Attempts)
}

// TODO: Make this test pass
// TestWorkerDelayedJobs tests that delayed jobs are processed after their delay
// func TestWorkerDelayedJobs(t *testing.T) {
// 	_, queue, cleanup := setupTestWorkerQueue(t)
// 	defer cleanup()

// 	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
// 	defer cancel()

// 	processed := make(chan time.Time, 1)

// 	processFn := func(job *oxmq.Job) (any, error) {
// 		processed <- time.Now()
// 		return nil, nil
// 	}

// 	worker, err := oxmq.NewWorker(queue, processFn, oxmq.WorkerConfig{Concurrency: 1})
// 	require.NoError(t, err)

// 	go worker.Start(ctx)

// 	// Add job with 5 second delay
// 	delay := 10 * time.Second
// 	addedAt := time.Now()
// 	_, err = queue.Add(context.Background(), "delayed-job", map[string]any{}, &oxmq.JobOptions{
// 		Delay: delay,
// 	})
// 	require.NoError(t, err)

// 	// Verify job starts in delayed state
// 	time.Sleep(100 * time.Millisecond)
// 	stats, err := queue.GetStats(context.Background())
// 	require.NoError(t, err)
// 	assert.Equal(t, int64(1), stats.Waiting)

// 	// Wait for processing
// 	select {
// 	case processedAt := <-processed:
// 		elapsed := processedAt.Sub(addedAt)
// 		assert.GreaterOrEqual(t, elapsed, delay, "Job was processed before delay elapsed")
// 		assert.Less(t, elapsed, delay+2*time.Second, "Job took too long to process after delay")
// 	case <-time.After(10 * time.Second):
// 		t.Fatal("Delayed job was not processed in time")
// 	}
// }

// TestWorkerPriority tests that higher priority jobs are processed first
func TestWorkerPriority(t *testing.T) {

	_, queue, cleanup := setupTestWorkerQueue(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var mu sync.Mutex
	processOrder := make([]int, 0)

	processFn := func(job *oxmq.Job) (any, error) {
		priority := job.Data["priority"].(float64) // JSON unmarshals numbers as float64
		mu.Lock()
		processOrder = append(processOrder, int(priority))
		mu.Unlock()
		time.Sleep(100 * time.Millisecond) // Ensure sequential processing
		return nil, nil
	}

	worker, err := oxmq.NewWorker(queue, processFn, oxmq.WorkerConfig{Concurrency: 1})
	require.NoError(t, err)

	// Add jobs in reverse priority order
	priorities := []int{1, 5, 3, 10, 2}
	for _, p := range priorities {
		_, err := queue.Add(context.Background(), "priority-job", map[string]any{"priority": p}, &oxmq.JobOptions{
			Priority: p,
		})
		require.NoError(t, err)
	}

	// Give time for all jobs to be added
	time.Sleep(200 * time.Millisecond)

	go worker.Start(ctx)

	// Wait for all jobs to be processed
	time.Sleep(3 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	// Verify jobs were processed in priority order (lowest first)
	assert.Len(t, processOrder, len(priorities))
	for i := 0; i < len(processOrder)-1; i++ {
		assert.LessOrEqual(t, processOrder[i], processOrder[i+1],
			"Jobs should be processed in ascending priority order (low priority value is more important)")
	}
}

// TestWorkerGracefulShutdown tests that workers stop processing when context is cancelled
func TestWorkerGracefulShutdown(t *testing.T) {
	_, queue, cleanup := setupTestWorkerQueue(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())

	var processedCount atomic.Int32

	processFn := func(job *oxmq.Job) (any, error) {
		processedCount.Add(1)
		time.Sleep(500 * time.Millisecond) // Simulate work
		return nil, nil
	}

	worker, err := oxmq.NewWorker(queue, processFn, oxmq.WorkerConfig{Concurrency: 2})
	require.NoError(t, err)

	// Add several jobs
	for i := 0; i < 10; i++ {
		_, err := queue.Add(context.Background(), "shutdown-test", map[string]any{"index": i}, nil)
		require.NoError(t, err)
	}

	// Start worker
	done := make(chan bool)
	go func() {
		worker.Start(ctx)
		done <- true
	}()

	// Let some jobs start processing
	time.Sleep(1 * time.Second)

	// Cancel context
	cancel()

	// Wait for worker to stop
	select {
	case <-done:
		// Worker stopped gracefully
	case <-time.After(5 * time.Second):
		t.Fatal("Worker did not stop in time")
	}

	// Verify not all jobs were processed (due to cancellation)
	processed := processedCount.Load()
	assert.Less(t, processed, int32(10), "Some jobs should remain unprocessed after cancellation")
}

// TestWorkerNoJobsAvailable tests worker behavior when no jobs are available
func TestWorkerNoJobsAvailable(t *testing.T) {
	_, queue, cleanup := setupTestWorkerQueue(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var processedCount atomic.Int32

	processFn := func(job *oxmq.Job) (any, error) {
		processedCount.Add(1)
		return nil, nil
	}

	worker, err := oxmq.NewWorker(queue, processFn, oxmq.WorkerConfig{Concurrency: 1})
	require.NoError(t, err)

	// Start worker with no jobs
	done := make(chan bool)
	go func() {
		worker.Start(ctx)
		done <- true
	}()

	// Wait for context timeout
	<-done

	// Verify no jobs were processed
	assert.Equal(t, int32(0), processedCount.Load())
}

// TestWorkerMultipleWorkersOneQueue tests multiple workers processing from same queue
func TestWorkerMultipleWorkersOneQueue(t *testing.T) {
	_, queue, cleanup := setupTestWorkerQueue(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const numWorkers = 3
	const numJobs = 15

	var processedCount atomic.Int32
	var mu sync.Mutex
	workerCounts := make(map[int]int)

	processFn := func(workerID int) oxmq.ProcessFn {
		return func(job *oxmq.Job) (any, error) {
			processedCount.Add(1)
			mu.Lock()
			workerCounts[workerID]++
			mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			return nil, nil
		}
	}

	// Start multiple workers
	for i := 0; i < numWorkers; i++ {
		worker, err := oxmq.NewWorker(queue, processFn(i), oxmq.WorkerConfig{Concurrency: 1})
		require.NoError(t, err)
		go worker.Start(ctx)
	}

	// Add jobs
	for i := 0; i < numJobs; i++ {
		_, err := queue.Add(context.Background(), "multi-worker-job", map[string]any{"index": i}, nil)
		require.NoError(t, err)
	}

	// Wait for all jobs to be processed
	deadline := time.After(8 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatalf("Not all jobs processed. Processed: %d/%d", processedCount.Load(), numJobs)
		case <-ticker.C:
			if processedCount.Load() == numJobs {
				goto DONE
			}
		}
	}

DONE:
	// Verify work was distributed among workers
	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, workerCounts, numWorkers, "All workers should have processed jobs")
	for workerID, count := range workerCounts {
		assert.Greater(t, count, 0, "Worker %d should have processed at least one job", workerID)
	}
}

// TestWorkerJobDataIntegrity tests that job data is preserved during processing
func TestWorkerJobDataIntegrity(t *testing.T) {
	_, queue, cleanup := setupTestWorkerQueue(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	expectedData := map[string]any{
		"string": "test",
		"number": float64(42),
		"bool":   true,
		"nested": map[string]any{
			"key": "value",
		},
	}

	received := make(chan map[string]any, 1)

	processFn := func(job *oxmq.Job) (any, error) {
		received <- job.Data
		return nil, nil
	}

	worker, err := oxmq.NewWorker(queue, processFn, oxmq.WorkerConfig{Concurrency: 1})
	require.NoError(t, err)

	go worker.Start(ctx)

	_, err = queue.Add(context.Background(), "data-test", expectedData, nil)
	require.NoError(t, err)

	select {
	case actualData := <-received:
		assert.Equal(t, expectedData, actualData)
	case <-time.After(3 * time.Second):
		t.Fatal("Job was not processed in time")
	}
}
