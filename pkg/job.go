package pkg

import (
	"encoding/json"
	"time"
)

type JobState string

const (
	JobWaiting   JobState = "waiting"
	JobActive    JobState = "active"
	JobCompleted JobState = "completed"
	JobFailed    JobState = "failed"
	JobDelayed   JobState = "delayed"
)

// Job represents a job in the queue
type Job struct {
	ID         string         `json:"id"`
	Name       string         `json:"name"`
	Data       map[string]any `json:"data"`
	State      JobState       `json:"state"`
	CreatedAt  time.Time      `json:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
	Error      string         `json:"error,omitempty"`
	Attempts   int            `json:"attempts"`
	MaxRetries int            `json:"max_retries"`
	Delay      time.Duration  `json:"delay,omitempty"`
	Priority   int            `json:"priority"`
	CustomID   string         `json:"custom_id,omitempty"`
}

// JobOptions represents options for a specific job
type JobOptions struct {
	MaxRetries int
	Delay      time.Duration
	Priority   int
	CustomID   string
}

// NewJob creates a new job with the given parameters
func NewJob(name string, data map[string]interface{}, opts *JobOptions) (*Job, error) {
	if name == "" {
		return nil, ErrEmptyJobName
	}

	if opts == nil {
		opts = &JobOptions{
			MaxRetries: 3,
			Priority:   0,
		}
	}

	jobID := generateJobID(name)

	return &Job{
		ID:         jobID,
		Name:       name,
		Data:       data,
		State:      JobWaiting,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
		Attempts:   0,
		MaxRetries: opts.MaxRetries,
		Delay:      opts.Delay,
		Priority:   opts.Priority,
		CustomID:   opts.CustomID,
	}, nil
}

func (j *Job) ToJSON() ([]byte, error) {
	return json.Marshal(j)
}

func (j *Job) FromJSON(data []byte) error {
	return json.Unmarshal(data, j)
}

// IsRetryable returns true if the job can be retried
func (j *Job) IsRetryable() bool {
	return j.Attempts < j.MaxRetries
}

// MarkFailed marks the job as failed with the given error
func (j *Job) MarkFailed(err error) {
	j.State = JobFailed
	j.Error = err.Error()
	j.UpdatedAt = time.Now()
}

// MarkCompleted marks the job as completed
func (j *Job) MarkCompleted() {
	j.State = JobCompleted
	j.Error = ""
	j.UpdatedAt = time.Now()
}

// IncrementAttempts increments the job attempt counter
func (j *Job) IncrementAttempts() {
	j.Attempts++
	j.UpdatedAt = time.Now()
}

func generateJobID(name string) string {
	return name + ":" + time.Now().Format(time.RFC3339Nano)
}
