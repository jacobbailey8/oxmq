package oxmq

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type WorkerConfig struct {
	Concurrency int
}

type ProcessFn func(*Job) (any, error)

type Worker struct {
	queue     *Queue
	processFn ProcessFn
	opts      WorkerConfig
	wg        sync.WaitGroup
	sem       chan struct{}
}

func NewWorker(queue *Queue, processFn ProcessFn, opts WorkerConfig) (*Worker, error) {
	if queue == nil {
		return nil, ErrNoQueueProvided
	}

	if opts.Concurrency < 0 {
		return nil, ErrNegConcurrency
	}

	if opts.Concurrency == 0 {
		opts.Concurrency = 1
	}

	return &Worker{
		queue:     queue,
		processFn: processFn,
		opts:      opts,
		sem:       make(chan struct{}, opts.Concurrency),
	}, nil
}

func (worker *Worker) Start(ctx context.Context) {

	// goroutine to receive ready jobs from waiting list
	worker.wg.Add(1)
	go func(ctx context.Context) {
		defer worker.wg.Done()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				item, err := worker.queue.client.BZPopMin(ctx, time.Second, worker.queue.keyGen.Waiting()).Result()
				if err != nil {
					if errors.Is(err, redis.Nil) {
						continue
					}
					return
				}
				jobId := item.Z.Member.(string)
				worker.sem <- struct{}{}
				worker.wg.Add(1)
				go processWaitingJob(ctx, worker, jobId)
			}
		}
	}(ctx)

}

func (worker *Worker) Stop() {
	worker.wg.Wait()
	close(worker.sem)
}

func processWaitingJob(ctx context.Context, worker *Worker, jobId string) {
	defer worker.wg.Done()

	for {
		select {
		case <-ctx.Done():
			// TODO:
			// if we canceled, place job back in waiting
			<-worker.sem // return worker to pool
			return
		default:
			// place job in active state, update `updated_at`
			job, err := worker.queue.PlaceJobInActive(ctx, jobId)
			if err != nil {
				return
			}

			// do user defined process function
			returnData, err := worker.processFn(job)

			// if process function returns an error, do a handle error function
			// that checks if it can be retried - if so increment attemts, place in waiting
			// if attempts exhausted, place in failed
			if err != nil {
				handleJobErrored(ctx, worker, job, err)
			} else {
				// if no error returns, place in completed and store return data in hash
				worker.queue.RemoveJobFromActive(ctx, job)
				worker.queue.PlaceJobInCompleted(ctx, job, returnData)
			}

			<-worker.sem // return worker to pool
			return
		}
	}
}

func handleJobErrored(ctx context.Context, worker *Worker, job *Job, err error) error {
	job.IncrementAttempts()
	var newErr error
	if job.IsRetryable() {
		// remove from active
		newErr = worker.queue.RemoveJobFromActive(ctx, job)
		if newErr != nil {
			return newErr
		}
		// place job in waiting
		newErr = worker.queue.PlaceJobInWaiting(ctx, job)
		if newErr != nil {
			return newErr
		}

	} else {
		// remove from active
		newErr = worker.queue.RemoveJobFromActive(ctx, job)
		if newErr != nil {
			return newErr
		}

		// place job in failed set and set error
		job.MarkFailed(err)

		// place in failed set
		newErr = worker.queue.PlaceJobInFailed(ctx, job)
		if newErr != nil {
			return newErr
		}

	}
	return nil
}
