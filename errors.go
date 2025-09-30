package oxmq

import "errors"

var (
	// ErrJobNotFound is returned when a job cannot be found
	ErrJobNotFound = errors.New("job not found")

	// ErrWorkerNotStarted is returned when trying to stop a worker that hasn't been started
	ErrWorkerNotStarted = errors.New("worker not started")

	// ErrWorkerAlreadyStarted is returned when trying to start a worker that's already running
	ErrWorkerAlreadyStarted = errors.New("worker already started")

	// ErrInvalidJobState is returned when trying to transition to an invalid state
	ErrInvalidJobState = errors.New("invalid job state")

	// ErrEmptyJobName is returned when trying to create a job without a name
	ErrEmptyJobName = errors.New("job name cannot be empty")
)
