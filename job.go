package oxmq

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type JobState string

const (
	JobWaiting   JobState = "waiting"
	JobActive    JobState = "active"
	JobCompleted JobState = "completed"
	JobFailed    JobState = "failed"
	JobDelayed   JobState = "delayed"
)

type Job struct {
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	Data        map[string]any `json:"data"`
	State       JobState       `json:"state"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	Error       string         `json:"error,omitempty"`
	Attempts    int            `json:"attempts"`
	MaxRetries  int            `json:"max_retries"`
	Delay       time.Duration  `json:"delay,omitempty"`
	Priority    int            `json:"priority"`
	CustomID    string         `json:"custom_id,omitempty"`
	ReturnValue any            `json:"return_value,omitempty"`
}

type JobOptions struct {
	MaxRetries int
	Delay      time.Duration
	Priority   int
	CustomID   string
}

func NewJob(name string, data map[string]any, opts *JobOptions) (*Job, error) {
	if name == "" {
		return nil, ErrEmptyJobName
	}

	if opts == nil {
		opts = &JobOptions{
			MaxRetries: 0,
			Priority:   0,
		}
	}

	var jobID string
	if opts.CustomID != "" {
		jobID = opts.CustomID
	} else {
		jobID = generateJobID(name)
	}

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

func (j *Job) IsRetryable() bool {
	return j.Attempts <= j.MaxRetries
}

func (j *Job) MarkFailed(err error) {
	j.State = JobFailed
	j.Error = err.Error()
	j.UpdatedAt = time.Now()
}

func (j *Job) MarkCompleted(returnData any) {
	j.State = JobCompleted
	j.Error = ""
	j.UpdatedAt = time.Now()
	j.ReturnValue = returnData
}

func (j *Job) IncrementAttempts() {
	j.Attempts++
	j.UpdatedAt = time.Now()
}

func generateJobID(name string) string {
	return fmt.Sprintf("%s:%s", name, uuid.NewString())
}
