package oxmq

import (
	"errors"
	"testing"
	"time"
)

func TestNewJob(t *testing.T) {
	tests := []struct {
		name    string
		jobName string
		data    map[string]any
		opts    *JobOptions
		wantErr error
	}{
		{"empty name", "", nil, nil, ErrEmptyJobName},
		{"basic job", "job1", map[string]any{"foo": "bar"}, nil, nil},
		{"with options", "job2", nil, &JobOptions{MaxRetries: 5, Delay: time.Second, Priority: 2, CustomID: "abc"}, nil},
		{"nil data", "job3", nil, nil, nil},
		{"empty data", "job4", map[string]any{}, nil, nil},
		{"complex data", "job5", map[string]any{"nested": map[string]any{"key": "value"}, "array": []int{1, 2, 3}}, nil, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job, err := NewJob(tt.jobName, tt.data, tt.opts)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("expected error %v, got %v", tt.wantErr, err)
			}
			if err != nil {
				return
			}

			// Verify fields
			if job.Name != tt.jobName {
				t.Errorf("expected name %s, got %s", tt.jobName, job.Name)
			}
			if job.State != JobWaiting {
				t.Errorf("expected state %s, got %s", JobWaiting, job.State)
			}
			if job.Attempts != 0 {
				t.Errorf("expected Attempts 0, got %d", job.Attempts)
			}
			if job.CreatedAt.IsZero() {
				t.Error("expected CreatedAt to be set")
			}
			if job.UpdatedAt.IsZero() {
				t.Error("expected UpdatedAt to be set")
			}
			if job.Error != "" {
				t.Errorf("expected empty error, got %s", job.Error)
			}

			// Verify options
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
				if tt.opts.CustomID != "" && job.ID != tt.opts.CustomID {
					t.Errorf("expected ID to match CustomID %s, got %s", tt.opts.CustomID, job.ID)
				}
			} else {
				// Default values when opts is nil
				if job.MaxRetries != 0 {
					t.Errorf("expected default MaxRetries 0, got %d", job.MaxRetries)
				}
				if job.Priority != 0 {
					t.Errorf("expected default Priority 0, got %d", job.Priority)
				}
			}

			// Verify ID generation
			if job.ID == "" {
				t.Error("expected non-empty ID")
			}
		})
	}
}

func TestNewJobWithCustomID(t *testing.T) {
	opts := &JobOptions{CustomID: "custom-123"}
	job, err := NewJob("test", nil, opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if job.ID != "custom-123" {
		t.Errorf("expected ID custom-123, got %s", job.ID)
	}
	if job.CustomID != "custom-123" {
		t.Errorf("expected CustomID custom-123, got %s", job.CustomID)
	}
}

func TestNewJobWithoutCustomID(t *testing.T) {
	job1, err := NewJob("test", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	time.Sleep(time.Millisecond) // Ensure different timestamps

	job2, err := NewJob("test", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if job1.ID == job2.ID {
		t.Error("expected different IDs for jobs created at different times")
	}
}

// Test JSON marshalling and unmarshalling
func TestJobJSON(t *testing.T) {
	now := time.Now()
	job := &Job{
		ID:          "123",
		Name:        "test",
		Data:        map[string]any{"foo": "bar", "num": float64(42)},
		State:       JobActive,
		CreatedAt:   now,
		UpdatedAt:   now,
		Error:       "some error",
		Attempts:    2,
		MaxRetries:  5,
		Delay:       time.Second,
		Priority:    3,
		CustomID:    "custom",
		ReturnValue: "result",
	}

	bytes, err := job.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}

	job2 := &Job{}
	if err := job2.FromJSON(bytes); err != nil {
		t.Fatalf("FromJSON failed: %v", err)
	}

	// Verify all fields
	if job2.ID != job.ID {
		t.Errorf("expected ID %s, got %s", job.ID, job2.ID)
	}
	if job2.Name != job.Name {
		t.Errorf("expected name %s, got %s", job.Name, job2.Name)
	}
	if job2.State != job.State {
		t.Errorf("expected state %s, got %s", job.State, job2.State)
	}
	if job2.Error != job.Error {
		t.Errorf("expected error %s, got %s", job.Error, job2.Error)
	}
	if job2.Attempts != job.Attempts {
		t.Errorf("expected attempts %d, got %d", job.Attempts, job2.Attempts)
	}
	if job2.MaxRetries != job.MaxRetries {
		t.Errorf("expected MaxRetries %d, got %d", job.MaxRetries, job2.MaxRetries)
	}
	if job2.Priority != job.Priority {
		t.Errorf("expected Priority %d, got %d", job.Priority, job2.Priority)
	}
	if job2.CustomID != job.CustomID {
		t.Errorf("expected CustomID %s, got %s", job.CustomID, job2.CustomID)
	}
	if job2.Data["foo"] != "bar" {
		t.Errorf("expected data foo=bar, got %v", job2.Data["foo"])
	}
}

func TestJobJSONWithNilData(t *testing.T) {
	job := &Job{
		ID:   "123",
		Name: "test",
		Data: nil,
	}

	bytes, err := job.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}

	job2 := &Job{}
	if err := job2.FromJSON(bytes); err != nil {
		t.Fatalf("FromJSON failed: %v", err)
	}

	if job2.Data != nil {
		t.Errorf("expected nil data, got %v", job2.Data)
	}
}

func TestJobFromJSONInvalidData(t *testing.T) {
	job := &Job{}
	err := job.FromJSON([]byte("invalid json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name       string
		attempts   int
		maxRetries int
		want       bool
	}{
		{"no attempts, retries available", 0, 3, true},
		{"some attempts, retries available", 1, 3, true},
		{"at max retries", 3, 3, false},
		{"exceeded max retries", 4, 3, false},
		{"no retries allowed", 0, 0, false},
		{"one attempt with one retry", 1, 1, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := &Job{Attempts: tt.attempts, MaxRetries: tt.maxRetries}
			got := job.IsRetryable()
			if got != tt.want {
				t.Errorf("expected IsRetryable() = %v, got %v", tt.want, got)
			}
		})
	}
}

func TestMarkFailed(t *testing.T) {
	job := &Job{
		State:     JobActive,
		UpdatedAt: time.Now().Add(-time.Hour),
	}
	originalUpdateTime := job.UpdatedAt
	testErr := errors.New("some error")

	time.Sleep(time.Millisecond) // Ensure UpdatedAt changes
	job.MarkFailed(testErr)

	if job.State != JobFailed {
		t.Errorf("expected state %s, got %s", JobFailed, job.State)
	}
	if job.Error != "some error" {
		t.Errorf("expected error 'some error', got %s", job.Error)
	}
	if !job.UpdatedAt.After(originalUpdateTime) {
		t.Error("expected UpdatedAt to be updated")
	}
}

func TestMarkFailedWithDifferentErrors(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		wantMsg string
	}{
		{"simple error", errors.New("fail"), "fail"},
		{"empty error message", errors.New(""), ""},
		{"complex error", errors.New("connection timeout: failed to connect to database"), "connection timeout: failed to connect to database"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := &Job{}
			job.MarkFailed(tt.err)

			if job.Error != tt.wantMsg {
				t.Errorf("expected error %s, got %s", tt.wantMsg, job.Error)
			}
			if job.State != JobFailed {
				t.Errorf("expected state %s, got %s", JobFailed, job.State)
			}
		})
	}
}

func TestMarkCompleted(t *testing.T) {
	job := &Job{
		State:     JobActive,
		Error:     "previous error",
		UpdatedAt: time.Now().Add(-time.Hour),
	}
	originalUpdateTime := job.UpdatedAt
	returnData := map[string]string{"result": "success"}

	time.Sleep(time.Millisecond) // Ensure UpdatedAt changes
	job.MarkCompleted(returnData)

	if job.State != JobCompleted {
		t.Errorf("expected state %s, got %s", JobCompleted, job.State)
	}
	if job.Error != "" {
		t.Errorf("expected empty error, got %s", job.Error)
	}
	if !job.UpdatedAt.After(originalUpdateTime) {
		t.Error("expected UpdatedAt to be updated")
	}
	if job.ReturnValue == nil {
		t.Error("expected ReturnValue to be set")
	}
}

func TestMarkCompletedWithDifferentReturnTypes(t *testing.T) {
	t.Run("nil return", func(t *testing.T) {
		job := &Job{Error: "previous error"}
		job.MarkCompleted(nil)

		if job.State != JobCompleted {
			t.Errorf("expected state %s, got %s", JobCompleted, job.State)
		}
		if job.Error != "" {
			t.Errorf("expected empty error, got %s", job.Error)
		}
		if job.ReturnValue != nil {
			t.Errorf("expected nil return value, got %v", job.ReturnValue)
		}
	})

	t.Run("string return", func(t *testing.T) {
		job := &Job{Error: "previous error"}
		job.MarkCompleted("success")

		if job.State != JobCompleted {
			t.Errorf("expected state %s, got %s", JobCompleted, job.State)
		}
		if job.ReturnValue != "success" {
			t.Errorf("expected return value 'success', got %v", job.ReturnValue)
		}
	})

	t.Run("int return", func(t *testing.T) {
		job := &Job{Error: "previous error"}
		job.MarkCompleted(42)

		if job.State != JobCompleted {
			t.Errorf("expected state %s, got %s", JobCompleted, job.State)
		}
		if job.ReturnValue != 42 {
			t.Errorf("expected return value 42, got %v", job.ReturnValue)
		}
	})

	t.Run("map return", func(t *testing.T) {
		job := &Job{Error: "previous error"}
		returnData := map[string]any{"key": "value"}
		job.MarkCompleted(returnData)

		if job.State != JobCompleted {
			t.Errorf("expected state %s, got %s", JobCompleted, job.State)
		}
		if job.ReturnValue == nil {
			t.Error("expected non-nil return value")
		}
		if m, ok := job.ReturnValue.(map[string]any); ok {
			if m["key"] != "value" {
				t.Errorf("expected key=value, got %v", m["key"])
			}
		} else {
			t.Error("expected return value to be a map")
		}
	})

	t.Run("slice return", func(t *testing.T) {
		job := &Job{Error: "previous error"}
		returnData := []int{1, 2, 3}
		job.MarkCompleted(returnData)

		if job.State != JobCompleted {
			t.Errorf("expected state %s, got %s", JobCompleted, job.State)
		}
		if job.ReturnValue == nil {
			t.Error("expected non-nil return value")
		}
		if s, ok := job.ReturnValue.([]int); ok {
			if len(s) != 3 || s[0] != 1 || s[1] != 2 || s[2] != 3 {
				t.Errorf("expected [1 2 3], got %v", s)
			}
		} else {
			t.Error("expected return value to be a slice")
		}
	})

	t.Run("struct return", func(t *testing.T) {
		job := &Job{Error: "previous error"}
		returnData := struct{ Name string }{"test"}
		job.MarkCompleted(returnData)

		if job.State != JobCompleted {
			t.Errorf("expected state %s, got %s", JobCompleted, job.State)
		}
		if job.ReturnValue == nil {
			t.Error("expected non-nil return value")
		}
	})
}

func TestIncrementAttempts(t *testing.T) {
	job := &Job{
		Attempts:  0,
		UpdatedAt: time.Now().Add(-time.Hour),
	}
	originalUpdateTime := job.UpdatedAt

	time.Sleep(time.Millisecond) // Ensure UpdatedAt changes
	job.IncrementAttempts()

	if job.Attempts != 1 {
		t.Errorf("expected Attempts 1, got %d", job.Attempts)
	}
	if !job.UpdatedAt.After(originalUpdateTime) {
		t.Error("expected UpdatedAt to be updated")
	}

	// Increment multiple times
	job.IncrementAttempts()
	job.IncrementAttempts()
	if job.Attempts != 3 {
		t.Errorf("expected Attempts 3, got %d", job.Attempts)
	}
}

func TestGenerateJobID(t *testing.T) {
	id1 := generateJobID("test")
	time.Sleep(time.Millisecond)
	id2 := generateJobID("test")

	if id1 == id2 {
		t.Error("expected different IDs for calls at different times")
	}

	if id1[:4] != "test" {
		t.Errorf("expected ID to start with 'test', got %s", id1)
	}

	// Test with different names
	id3 := generateJobID("other")
	if id3[:5] != "other" {
		t.Errorf("expected ID to start with 'other', got %s", id3)
	}
}

func TestJobStateTransitions(t *testing.T) {
	job := &Job{State: JobWaiting}

	// Waiting -> Active
	job.State = JobActive
	if job.State != JobActive {
		t.Error("failed to transition to Active state")
	}

	// Active -> Completed
	job.MarkCompleted(nil)
	if job.State != JobCompleted {
		t.Error("failed to transition to Completed state")
	}

	// Reset and test failure path
	job.State = JobActive
	job.MarkFailed(errors.New("error"))
	if job.State != JobFailed {
		t.Error("failed to transition to Failed state")
	}
}

func TestJobWithAllStates(t *testing.T) {
	states := []JobState{JobWaiting, JobActive, JobCompleted, JobFailed, JobDelayed}

	for _, state := range states {
		job := &Job{State: state}
		if job.State != state {
			t.Errorf("expected state %s, got %s", state, job.State)
		}
	}
}

func TestJobPriority(t *testing.T) {
	jobs := []*Job{
		{Priority: 0},
		{Priority: 1},
		{Priority: -1},
		{Priority: 100},
	}

	for _, job := range jobs {
		if job.Priority < -1 || job.Priority > 100 {
			// This is just to show priority can be any int
			t.Logf("Job has priority outside common range: %d", job.Priority)
		}
	}
}

func TestJobDelay(t *testing.T) {
	tests := []struct {
		name  string
		delay time.Duration
	}{
		{"no delay", 0},
		{"one second", time.Second},
		{"one minute", time.Minute},
		{"one hour", time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &JobOptions{Delay: tt.delay}
			job, err := NewJob("test", nil, opts)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if job.Delay != tt.delay {
				t.Errorf("expected delay %v, got %v", tt.delay, job.Delay)
			}
		})
	}
}
