package pkg

import (
	"context"
	"fmt"
	"time"

	"github.com/jacobbailey8/oxmq/internal/redis/keys"
	"github.com/redis/go-redis/v9"
)

// Queue represents a job queue
type Queue struct {
	name   string
	client *redis.Client
	keyGen *keys.KeyGenerator
}

// QueueStats represents queue statistics
type QueueStats struct {
	Waiting   int64 `json:"waiting"`
	Active    int64 `json:"active"`
	Completed int64 `json:"completed"`
	Failed    int64 `json:"failed"`
	Delayed   int64 `json:"delayed"`
}

// NewQueue creates a new queue instance
func NewQueue(name string, client *redis.Client) *Queue {
	return &Queue{
		name:   name,
		client: client,
		keyGen: keys.NewKeyGenerator(name),
	}
}

// Add adds a new job to the queue
func (q *Queue) Add(ctx context.Context, name string, data map[string]any, opts *JobOptions) (*Job, error) {

	job, err := NewJob(name, data, opts)
	if err != nil {
		return nil, err
	}

	return q.addJob(ctx, job)
}

// AddJob adds an existing job to the queue
func (q *Queue) AddJob(ctx context.Context, job *Job) (*Job, error) {
	return q.addJob(ctx, job)
}

func (q *Queue) addJob(ctx context.Context, job *Job) (*Job, error) {
	jobData, err := job.ToJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal job: %w", err)
	}

	pipe := q.client.Pipeline()

	// Store job data
	pipe.HSet(ctx, q.keyGen.Job(job.ID), "data", jobData)

	if job.Delay > 0 {
		// Add to delayed set with score as timestamp when job should be processed
		delayedUntil := time.Now().Add(job.Delay).Unix()
		pipe.ZAdd(ctx, q.keyGen.Delayed(), redis.Z{
			Score:  float64(delayedUntil),
			Member: job.ID,
		})
		job.State = JobDelayed
	} else {
		// Add to waiting list
		pipe.LPush(ctx, q.keyGen.Waiting(), job.ID)
	}

	// Add to state set
	pipe.SAdd(ctx, q.keyGen.State(string(job.State)), job.ID)

	_, err = pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to add job to queue: %w", err)
	}

	return job, nil
}

// GetJob retrieves a job by ID
func (q *Queue) GetJob(ctx context.Context, jobID string) (*Job, error) {
	data, err := q.client.HGet(ctx, q.keyGen.Job(jobID), "data").Result()
	if err != nil {
		if err == redis.Nil {
			return nil, ErrJobNotFound
		}
		return nil, err
	}

	var job Job
	err = job.FromJSON([]byte(data))
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	return &job, nil
}

// GetJobs returns jobs by state
func (q *Queue) GetJobs(ctx context.Context, state JobState, start, stop int64) ([]*Job, error) {
	var jobIDs []string
	var err error

	if start < 0 || stop < 0 {
		jobIDs, err = q.client.SMembers(ctx, q.keyGen.State(string(state))).Result()
	} else {
		jobIDs, err = q.client.SRandMemberN(ctx, q.keyGen.State(string(state)), stop-start+1).Result()
	}

	if err != nil {
		return nil, err
	}

	jobs := make([]*Job, 0, len(jobIDs))
	for _, jobID := range jobIDs {
		job, err := q.GetJob(ctx, jobID)
		if err != nil {
			continue // Skip jobs that can't be retrieved
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}

func (q *Queue) GetWaiting(ctx context.Context, start, stop int64) ([]*Job, error) {
	return q.GetJobs(ctx, JobWaiting, start, stop)
}

func (q *Queue) GetActive(ctx context.Context, start, stop int64) ([]*Job, error) {
	return q.GetJobs(ctx, JobActive, start, stop)
}

func (q *Queue) GetCompleted(ctx context.Context, start, stop int64) ([]*Job, error) {
	return q.GetJobs(ctx, JobCompleted, start, stop)
}

func (q *Queue) GetFailed(ctx context.Context, start, stop int64) ([]*Job, error) {
	return q.GetJobs(ctx, JobFailed, start, stop)
}

// Count returns the number of jobs in a specific state
func (q *Queue) Count(ctx context.Context, state JobState) (int64, error) {
	return q.client.SCard(ctx, q.keyGen.State(string(state))).Result()
}

// GetStats returns queue statistics
func (q *Queue) GetStats(ctx context.Context) (*QueueStats, error) {
	states := []JobState{JobWaiting, JobActive, JobCompleted, JobFailed, JobDelayed}
	counts := make([]int64, len(states))

	pipe := q.client.Pipeline()
	cmds := make([]*redis.IntCmd, len(states))

	for i, state := range states {
		cmds[i] = pipe.SCard(ctx, q.keyGen.State(string(state)))
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}

	for i, cmd := range cmds {
		counts[i] = cmd.Val()
	}

	return &QueueStats{
		Waiting:   counts[0],
		Active:    counts[1],
		Completed: counts[2],
		Failed:    counts[3],
		Delayed:   counts[4],
	}, nil
}

// Clean removes jobs older than the specified duration
func (q *Queue) Clean(ctx context.Context, maxAge time.Duration, state JobState, limit int) (int, error) {
	jobs, err := q.GetJobs(ctx, state, 0, int64(limit))
	if err != nil {
		return 0, err
	}

	cleaned := 0
	cutoff := time.Now().Add(-maxAge)

	for _, job := range jobs {
		if job.UpdatedAt.Before(cutoff) {
			err := q.RemoveJob(ctx, job.ID)
			if err == nil {
				cleaned++
			}
		}
	}

	return cleaned, nil
}

// RemoveJob completely removes a job from the queue
func (q *Queue) RemoveJob(ctx context.Context, jobID string) error {
	job, err := q.GetJob(ctx, jobID)
	if err != nil {
		return err
	}

	pipe := q.client.Pipeline()

	// Remove from all possible locations
	pipe.SRem(ctx, q.keyGen.State(string(job.State)), jobID)
	pipe.LRem(ctx, q.keyGen.Waiting(), 1, jobID)
	pipe.LRem(ctx, q.keyGen.Active(), 1, jobID)
	pipe.ZRem(ctx, q.keyGen.Delayed(), jobID)
	pipe.Del(ctx, q.keyGen.Job(jobID))

	_, err = pipe.Exec(ctx)
	return err
}

// moveJobToState moves a job from one state to another
func (q *Queue) moveJobToState(ctx context.Context, jobID string, fromState, toState JobState) error {
	pipe := q.client.Pipeline()

	// Remove from old state
	if fromState != "" {
		pipe.SRem(ctx, q.keyGen.State(string(fromState)), jobID)
	}

	// Add to new state
	pipe.SAdd(ctx, q.keyGen.State(string(toState)), jobID)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}

	// Update job state
	job, err := q.GetJob(ctx, jobID)
	if err != nil {
		return err
	}

	job.State = toState
	job.UpdatedAt = time.Now()

	return q.updateJob(ctx, job)
}

// updateJob updates job data in Redis
func (q *Queue) updateJob(ctx context.Context, job *Job) error {
	jobData, err := job.ToJSON()
	if err != nil {
		return err
	}

	return q.client.HSet(ctx, q.keyGen.Job(job.ID), "data", jobData).Err()
}

// Pause pauses the queue (prevents new jobs from being processed)
func (q *Queue) Pause(ctx context.Context) error {
	return q.client.Set(ctx, q.keyGen.Paused(), "1", 0).Err()
}

// Resume resumes the queue
func (q *Queue) Resume(ctx context.Context) error {
	return q.client.Del(ctx, q.keyGen.Paused()).Err()
}

// IsPaused returns true if the queue is paused
func (q *Queue) IsPaused(ctx context.Context) (bool, error) {
	result := q.client.Exists(ctx, q.keyGen.Paused())
	return result.Val() > 0, result.Err()
}

// Close closes the queue and cleans up resources
func (q *Queue) Close() error {
	// Implement any cleanup logic here
	return nil
}

// Removes all jobs in waiting or delayed state
func (q *Queue) Drain() error

func (q *Queue) Obliterate() error

// type Queue struct {
// 	name          string
// 	jobQueue      safePriorityQueue
// 	jobSet        set
// 	mu            sync.Mutex
// 	keepCompleted int
// 	keepFailed    int
// }

// func (q *Queue) AddJobs(jobs []*Job) {
// 	q.mu.Lock()
// 	defer q.mu.Unlock()
// 	q.jobQueue = append(q.jobQueue, jobs...)
// }

// func (q *Queue) GetPrioritizedJobs() []*Job {
// 	prioritizedJobs := make([]*Job, 0)
// 	q.mu.Lock()
// 	defer q.mu.Unlock()
// 	for _, job := range q.jobQueue {
// 		if job.priority > 0 {
// 			prioritizedJobs = append(prioritizedJobs, job)
// 		}
// 	}
// 	return prioritizedJobs
// }
