package oxmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jacobbailey8/oxmq/internal/redis/keys"
	"github.com/redis/go-redis/v9"
)

type Queue struct {
	name   string
	client *redis.Client
	keyGen *keys.KeyGenerator
}

type QueueStats struct {
	Waiting   int64 `json:"waiting"`
	Active    int64 `json:"active"`
	Completed int64 `json:"completed"`
	Failed    int64 `json:"failed"`
	Delayed   int64 `json:"delayed"`
}

func NewQueue(name string, client *redis.Client) *Queue {
	return &Queue{
		name:   name,
		client: client,
		keyGen: keys.NewKeyGenerator(name),
	}
}

func (q *Queue) Client() *redis.Client {
	return q.client
}

func (q *Queue) KeyGen() *keys.KeyGenerator {
	return q.keyGen
}

// Adds a new job to the queue based on explicit parameters
func (q *Queue) Add(ctx context.Context, name string, data map[string]any, opts *JobOptions) (*Job, error) {

	job, err := NewJob(name, data, opts)
	if err != nil {
		return nil, err
	}

	return q.addJob(ctx, job)
}

// Adds an existing job to the queue
func (q *Queue) AddJob(ctx context.Context, job *Job) (*Job, error) {
	return q.addJob(ctx, job)
}

// Adds a slice of jobs to a queue concurrently
func (q *Queue) AddBulk(ctx context.Context, jobs []*Job) {
	const maxWorkers = 500
	jobsCh := make(chan *Job, len(jobs))
	var group sync.WaitGroup

	// Start worker pool
	for range maxWorkers {
		group.Go(func() {
			for job := range jobsCh {
				q.addJob(ctx, job)
			}
		})
	}

	// Feed jobs into the channel
	for _, job := range jobs {
		jobsCh <- job
	}
	close(jobsCh)
	group.Wait()
}

func (q *Queue) addJob(ctx context.Context, job *Job) (*Job, error) {
	jobData, err := job.ToJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal job: %w", err)
	}

	pipe := q.client.TxPipeline()

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
		// Add to waiting list with priority: timestamp appended to priority score
		// this will respect priority score primarily and preserve order by insertion for ties
		score := float64(job.Priority)*1e12 + float64(time.Now().UnixNano())
		pipe.ZAdd(ctx, q.keyGen.Waiting(), redis.Z{
			Score:  score,
			Member: job.ID,
		})
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to add job to queue: %w", err)
	}

	return job, nil
}

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

// Returns jobs by state.
// - For waiting/delayed (sorted sets), it uses ZRANGE or ZRANGEBYSCORE.
// - For other states (sets), it uses SMEMBERS or SRANDMEMBERN.
func (q *Queue) GetJobs(ctx context.Context, state JobState, start, stop int64) ([]*Job, error) {
	var jobIDs []string
	var err error

	switch state {
	case JobWaiting, JobDelayed:
		// For sorted sets, preserve score ordering
		if start < 0 || stop < 0 {
			// Default to all
			jobIDs, err = q.client.ZRange(ctx, q.keyGen.State(string(state)), 0, -1).Result()
		} else {
			jobIDs, err = q.client.ZRange(ctx, q.keyGen.State(string(state)), start, stop).Result()
		}
	default:
		// For sets (unordered states like active, completed, failed)
		if start < 0 || stop < 0 {
			jobIDs, err = q.client.SMembers(ctx, q.keyGen.State(string(state))).Result()
		} else {
			jobIDs, err = q.client.SRandMemberN(ctx, q.keyGen.State(string(state)), stop-start+1).Result()
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to fetch job IDs for state %s: %w", state, err)
	}

	// Fetch job objects
	jobs := make([]*Job, 0, len(jobIDs))
	for _, jobID := range jobIDs {
		job, err := q.GetJob(ctx, jobID)
		if err != nil {
			// Skip corrupted/missing jobs
			continue
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

// Returns the number of jobs in a specific state.
func (q *Queue) Count(ctx context.Context, state JobState) (int64, error) {
	switch state {
	case JobWaiting, JobDelayed:
		return q.client.ZCard(ctx, q.keyGen.State(string(state))).Result()
	default:
		return q.client.SCard(ctx, q.keyGen.State(string(state))).Result()
	}
}

// GetStats returns the number of jobs in each state
func (q *Queue) GetStats(ctx context.Context) (*QueueStats, error) {
	states := []JobState{JobWaiting, JobActive, JobCompleted, JobFailed, JobDelayed}
	counts := make([]int64, len(states))

	pipe := q.client.TxPipeline()
	cmds := make([]*redis.IntCmd, len(states))

	for i, state := range states {
		switch state {
		case JobWaiting, JobDelayed:
			cmds[i] = pipe.ZCard(ctx, q.keyGen.State(string(state)))
		default:
			cmds[i] = pipe.SCard(ctx, q.keyGen.State(string(state)))
		}
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

// Removes jobs older than the specified duration
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

func (q *Queue) RemoveJob(ctx context.Context, jobID string) error {
	job, err := q.GetJob(ctx, jobID)
	if err != nil {
		return err
	}

	pipe := q.client.TxPipeline()

	switch job.State {
	case JobWaiting, JobDelayed:
		pipe.ZRem(ctx, q.keyGen.State(string(job.State)), jobID)
	default:
		pipe.SRem(ctx, q.keyGen.State(string(job.State)), jobID)
	}

	pipe.Del(ctx, q.keyGen.Job(jobID))

	_, err = pipe.Exec(ctx)
	return err
}

func (q *Queue) PlaceJobInActive(ctx context.Context, jobID string) (*Job, error) {
	pipe := q.client.TxPipeline()
	var job Job
	jobData, err := q.client.HGet(ctx, q.keyGen.Job(jobID), "data").Result()
	if err != nil {
		return nil, err
	}
	if err := job.FromJSON([]byte(jobData)); err != nil {
		return nil, err
	}

	if job.State != JobWaiting {
		return nil, fmt.Errorf("expected job to be in waiting state, was in %s", job.State)
	}

	// update job data
	job.UpdatedAt = time.Now()
	job.State = JobActive

	// Update job in hash
	newJobData, err := job.ToJSON()
	if err != nil {
		return nil, err
	}
	pipe.HSet(ctx, q.keyGen.Job(jobID), "data", newJobData)

	// place job in active
	pipe.SAdd(ctx, q.keyGen.Active(), jobID)

	_, err = pipe.Exec(ctx)
	return &job, err
}

func (q *Queue) PlaceJobInWaiting(ctx context.Context, job *Job) error {
	pipe := q.client.TxPipeline()

	// update job data
	job.UpdatedAt = time.Now()
	job.State = JobWaiting

	// Update job in hash
	newJobData, err := job.ToJSON()
	if err != nil {
		return err
	}
	pipe.HSet(ctx, q.keyGen.Job(job.ID), "data", newJobData)

	// place job in waiting
	score := float64(job.Priority)*1e12 + float64(time.Now().UnixNano())
	pipe.ZAdd(ctx, q.keyGen.Waiting(), redis.Z{
		Score:  score,
		Member: job.ID,
	})

	_, err = pipe.Exec(ctx)
	return err
}

func (q *Queue) PlaceJobInFailed(ctx context.Context, job *Job) error {
	pipe := q.client.TxPipeline()

	// Update job in hash
	newJobData, err := job.ToJSON()
	if err != nil {
		return err
	}
	pipe.HSet(ctx, q.keyGen.Job(job.ID), "data", newJobData)

	// place job in failed
	pipe.SAdd(ctx, q.keyGen.Failed(), job.ID)

	_, err = pipe.Exec(ctx)
	return err
}

func (q *Queue) RemoveJobFromActive(ctx context.Context, job *Job) error {
	pipe := q.client.TxPipeline()

	job.UpdatedAt = time.Now()
	job.State = ""

	// Update job in hash
	newJobData, err := job.ToJSON()
	if err != nil {
		return err
	}
	pipe.HSet(ctx, q.keyGen.Job(job.ID), "data", newJobData)

	// remove from actuve
	pipe.SRem(ctx, q.keyGen.Active(), job.ID)

	_, err = pipe.Exec(ctx)
	return err
}

func (q *Queue) PlaceJobInCompleted(ctx context.Context, job *Job, returnData any) error {
	pipe := q.client.TxPipeline()

	job.MarkCompleted(returnData)

	// Update job in hash
	newJobData, err := job.ToJSON()
	if err != nil {
		return err
	}
	pipe.HSet(ctx, q.keyGen.Job(job.ID), "data", newJobData)

	// place in completed set
	pipe.SAdd(ctx, q.keyGen.Completed(), job.ID)

	_, err = pipe.Exec(ctx)
	return err
}

// func (q *Queue) moveJobToState(ctx context.Context, jobID string, fromState, toState JobState) (*Job, error) {

// 	const maxRetries = 5

// 	for i := range maxRetries {
// 		var job Job
// 		// Watch makes the entire transaction atomic in redis
// 		err := q.client.Watch(ctx, func(tx *redis.Tx) error {
// 			jobData, err := tx.HGet(ctx, q.keyGen.Job(jobID), "data").Result()
// 			if err != nil {
// 				return err
// 			}
// 			if err := job.FromJSON([]byte(jobData)); err != nil {
// 				return err
// 			}

// 			if job.State != fromState {
// 				return fmt.Errorf("expected job in %s but found %s", fromState, job.State)
// 			}

// 			pipe := tx.TxPipeline()

// 			// Remove from old state
// 			if fromState != "" {
// 				switch fromState {
// 				case JobWaiting, JobDelayed:
// 					pipe.ZRem(ctx, q.keyGen.State(string(fromState)), jobID)
// 				default:
// 					pipe.SRem(ctx, q.keyGen.State(string(fromState)), jobID)
// 				}
// 			}

// 			// Places job in correct state set/zset
// 			switch toState {
// 			case JobWaiting:
// 				score := float64(job.Priority)*1e12 + float64(time.Now().UnixNano())
// 				pipe.ZAdd(ctx, q.keyGen.Waiting(), redis.Z{
// 					Score:  score,
// 					Member: job.ID,
// 				})
// 			case JobDelayed:
// 				delayedUntil := time.Now().Add(job.Delay).Unix()
// 				pipe.ZAdd(ctx, q.keyGen.Delayed(), redis.Z{
// 					Score:  float64(delayedUntil),
// 					Member: job.ID,
// 				})
// 			default:
// 				pipe.SAdd(ctx, q.keyGen.State(string(toState)), jobID)
// 			}

// 			job.State = toState
// 			job.UpdatedAt = time.Now()

// 			// Update job in hash
// 			newJobData, err := job.ToJSON()
// 			if err != nil {
// 				return err
// 			}
// 			pipe.HSet(ctx, q.keyGen.Job(job.ID), "data", newJobData)

// 			_, err = pipe.Exec(ctx)
// 			return err
// 		}, q.keyGen.Job(jobID))

// 		if err == nil {
// 			return &job, nil
// 		}

// 		if err == redis.TxFailedErr {
// 			// Another client modified the key, retry after a small backoff
// 			time.Sleep(time.Duration(i+1) * 10 * time.Millisecond)
// 			continue
// 		}

// 		// Any other error is fatal
// 		return nil, err
// 	}

// 	return nil, fmt.Errorf("moveJobToState failed after %d retries due to concurrent modifications", maxRetries)
// }

// Pauses the queue (prevents new jobs from being processed)
func (q *Queue) Pause(ctx context.Context) error {
	return q.client.Set(ctx, q.keyGen.Paused(), "1", 0).Err()
}

func (q *Queue) Resume(ctx context.Context) error {
	return q.client.Del(ctx, q.keyGen.Paused()).Err()
}

func (q *Queue) IsPaused(ctx context.Context) (bool, error) {
	result := q.client.Exists(ctx, q.keyGen.Paused())
	return result.Val() > 0, result.Err()
}

// Removes all jobs in waiting or delayed state.
func (q *Queue) Drain(ctx context.Context) error {
	pipe := q.client.TxPipeline()

	// Fetch all waiting jobs
	waitingIDs, err := q.client.ZRange(ctx, q.keyGen.Waiting(), 0, -1).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("failed to fetch waiting jobs: %w", err)
	}

	// Fetch all delayed jobs
	delayedIDs, err := q.client.ZRange(ctx, q.keyGen.Delayed(), 0, -1).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("failed to fetch delayed jobs: %w", err)
	}

	allIDs := append(waitingIDs, delayedIDs...)

	// Remove job hashes
	for _, jobID := range allIDs {
		pipe.Del(ctx, q.keyGen.Job(jobID))
	}

	// Clear the waiting and delayed sets
	pipe.Del(ctx, q.keyGen.Waiting())
	pipe.Del(ctx, q.keyGen.Delayed())

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to drain queue: %w", err)
	}

	return nil
}

func (q *Queue) Obliterate(ctx context.Context) error {
	pipe := q.client.TxPipeline()

	states := []JobState{JobWaiting, JobDelayed, JobActive, JobCompleted, JobFailed}
	var allJobKeys []string

	// Get all jobs in each state
	for _, state := range states {
		jobs, err := q.GetJobs(ctx, state, -1, -1)
		if err != nil {
			return err
		}
		for _, job := range jobs {
			allJobKeys = append(allJobKeys, q.keyGen.Job(job.ID))
		}
		pipe.Del(ctx, q.keyGen.State(string(state)))
	}

	// Delete all job hashes
	if len(allJobKeys) > 0 {
		pipe.Del(ctx, allJobKeys...)
	}

	// Delete metadata & paused flag
	pipe.Del(ctx, q.keyGen.Meta())
	pipe.Del(ctx, q.keyGen.Paused())

	_, err := pipe.Exec(ctx)
	return err
}
