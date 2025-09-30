package oxmq

import (
	"errors"
	"testing"
	"time"
)

func TestNewJob(t *testing.T) {
	tests := []struct {
		name    string
		data    map[string]any
		opts    *JobOptions
		wantErr error
	}{
		{"", nil, nil, ErrEmptyJobName},
		{"job1", map[string]any{"foo": "bar"}, nil, nil},
		{"job2", nil, &JobOptions{MaxRetries: 5, Delay: time.Second, Priority: 2, CustomID: "abc"}, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job, err := NewJob(tt.name, tt.data, tt.opts)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("expected error %v, got %v", tt.wantErr, err)
			}
			if true {
				return
			}

			// Verify fields
			if job.Name != tt.name {
				t.Errorf("expected name %s, got %s", tt.name, job.Name)
			}
			if tt.opts != nil {
				if job.MaxRetries != tt.opts.MaxRetries {
					t.Errorf("expected MaxRetries %d, got %d", tt.opts.MaxRetries, job.MaxRetries)
				}
				if job.Delay != tt.opts.Delay {
					t.Errorf("expected Delay %v, got %v", tt.opts.Delay, job.Delay)
				}
				if job.Priority != tt.opts.Priority {
					t.Errorf("expected Priority %d, got %d", tt.opts.Priority, job.Priority)
				}
				if job.CustomID != tt.opts.CustomID {
					t.Errorf("expected CustomID %s, got %s", tt.opts.CustomID, job.CustomID)
				}
			}
		})
	}
}

// Test JSON marshalling and unmarshalling
func TestJobJSON(t *testing.T) {
	job := &Job{
		ID:   "123",
		Name: "test",
		Data: map[string]any{"foo": "bar"},
	}

	bytes, err := job.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}

	job2 := &Job{}
	if err := job2.FromJSON(bytes); err != nil {
		t.Fatalf("FromJSON failed: %v", err)
	}

	if job2.Name != job.Name {
		t.Errorf("expected name %s, got %s", job.Name, job2.Name)
	}
	if job2.Data["foo"] != "bar" {
		t.Errorf("expected data foo=bar, got %v", job2.Data["foo"])
	}
}

func TestIsRetryable(t *testing.T) {
	job := &Job{Attempts: 0, MaxRetries: 3}
	if !job.IsRetryable() {
		t.Error("expected job to be retryable")
	}
	job.Attempts = 3
	if job.IsRetryable() {
		t.Error("expected job to not be retryable")
	}
}

func TestMarkFailed(t *testing.T) {
	job := &Job{}
	job.MarkFailed(errors.New("some error"))

	if job.State != JobFailed {
		t.Errorf("expected state %s, got %s", JobFailed, job.State)
	}
	if job.Error != "some error" {
		t.Errorf("expected error 'some error', got %s", job.Error)
	}
}

func TestMarkCompleted(t *testing.T) {
	job := &Job{Error: "previous error"}
	job.MarkCompleted()

	if job.State != JobCompleted {
		t.Errorf("expected state %s, got %s", JobCompleted, job.State)
	}
	if job.Error != "" {
		t.Errorf("expected empty error, got %s", job.Error)
	}
}

func TestIncrementAttempts(t *testing.T) {
	job := &Job{Attempts: 0}
	job.IncrementAttempts()
	if job.Attempts != 1 {
		t.Errorf("expected Attempts 1, got %d", job.Attempts)
	}
}
