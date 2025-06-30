package worker

import (
	"context"
	"fmt"
	"jobqueue/entity"
	_interface "jobqueue/interface"
	"jobqueue/pkg/constant"
	"log"
	"sync"
	"time"
)

type JobWorker struct {
	jobRepo    _interface.JobRepository
	options    constant.WorkerOptions
	stopChan   chan struct{}
	wg         sync.WaitGroup
	workerPool chan struct{}
}

// NewJobWorker creates a new instance of JobWorker.
func NewJobWorker(repo _interface.JobRepository, options constant.WorkerOptions) *JobWorker {
	return &JobWorker{
		jobRepo:    repo,
		options:    options,
		stopChan:   make(chan struct{}),
		workerPool: make(chan struct{}, options.MaxWorkers),
	}
}

// Start begins the worker's process of polling and processing jobs.
func (jw *JobWorker) Start(ctx context.Context) {
	log.Println("JobWorker started. Polling for jobs...")
	jw.wg.Add(1)
	go func() {
		defer jw.wg.Done()
		ticker := time.NewTicker(jw.options.PollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Println("JobWorker context cancelled. Stopping polling.")
				return
			case <-jw.stopChan:
				log.Println("JobWorker received stop signal. Stopping polling.")
				return
			case <-ticker.C:
				jw.pullAndProcessJobs(ctx)
			}
		}
	}()
}

// Stop gracefully stops the worker.
func (jw *JobWorker) Stop() {
	log.Println("Stopping JobWorker...")
	close(jw.stopChan)
	jw.wg.Wait()
	log.Println("JobWorker stopped.")
}

// pollAndProcessJobs retrieves pending jobs and dispatches them to worker goroutines.
func (jw *JobWorker) pullAndProcessJobs(ctx context.Context) {
	jobs, err := jw.jobRepo.FindAll(ctx)
	if err != nil {
		log.Printf("Error polling jobs: %v", err)
		return
	}

	for _, job := range jobs {
		// Only process jobs that are pending and haven't exceeded max attempts
		// For retry, we set status back to pending and use delay
		if job.Status == "pending" && job.Attempts < jw.options.MaxAttempts {
			log.Printf("Found pending job: %s (Task: %s, Attempts: %d)", job.ID, job.Task, job.Attempts)

			// Try to acquire a slot in the worker pool
			select {
			case jw.workerPool <- struct{}{}:
				jw.wg.Add(1)
				go func(j *entity.Job) {
					defer jw.wg.Done()
					defer func() { <-jw.workerPool }()

					jw.processJob(ctx, j)
				}(job)
			case <-ctx.Done():
				log.Println("Context cancelled while waiting for worker slot. Stopping job polling.")
				return
			default:
				// Worker pool is full, skip for now.
				log.Printf("Worker pool full. Skipping job %s (Task: %s) for now.", job.ID, job.Task)
			}
		} else if job.Status == "pending" && job.Attempts >= jw.options.MaxAttempts {
			job.Status = "failed"
			if err := jw.jobRepo.Save(ctx, job); err != nil {
				log.Printf("Error marking job %s as failed (attempts exceeded): %v", job.ID, err)
			}
			log.Printf("Job %s (Task: %s) moved to failed status as max attempts (%d) reached during polling.", job.ID, job.Task, jw.options.MaxAttempts)
		}
	}
}

// processJob contains the core logic for executing a job.
func (jw *JobWorker) processJob(ctx context.Context, job *entity.Job) {
	log.Printf("Processing job: %s (Task: %s, Token: %s, Attempts: %d)", job.ID, job.Task, job.Token, job.Attempts)

	job.Status = "running"
	if err := jw.jobRepo.Save(ctx, job); err != nil {
		log.Printf("Error updating job %s to running: %v", job.ID, err)
		return
	}

	var err error
	if job.Task == "unstable-job" {
		log.Printf("Handling unstable job: %s. Current attempts: %d", job.ID, job.Attempts)
		if job.Attempts < 2 { // Fail for the first two attempts
			err = fmt.Errorf("simulated unstable job failure (attempt %d)", job.Attempts+1)
			log.Printf("Unstable job %s failed on attempt %d.", job.ID, job.Attempts+1)
		} else {
			// Succeed at third attempt
			log.Printf("Unstable job %s succeeded after %d attempts.", job.ID, job.Attempts+1)
			err = nil
		}
	} else {
		// Simulate execution for a normal job
		log.Printf("Executing normal job: %s", job.Task)
		time.Sleep(500 * time.Millisecond)
		err = nil
	}

	if err != nil {
		log.Printf("Job %s (Task: %s) failed: %v", job.ID, job.Task, err)
		job.Attempts++

		if job.Attempts < jw.options.MaxAttempts {
			job.Status = "pending"
			log.Printf("Retrying job %s (Attempts: %d/%d). Delaying for %v before next poll...",
				job.ID, job.Attempts, jw.options.MaxAttempts, jw.options.RetryDelay)

			time.Sleep(jw.options.RetryDelay)
		} else {
			job.Status = "failed" // Permanently failed if exceed max attempts
			log.Printf("Job %s (Task: %s) failed permanently after %d attempts. IdempotencyKey: %s", job.ID, job.Task, job.Attempts, job.Token)
		}
	} else {
		job.Status = "completed"
		log.Printf("Job %s (Task: %s) completed successfully. Token: %s", job.ID, job.Task, job.Token)
	}

	// Save
	if err := jw.jobRepo.Save(ctx, job); err != nil {
		log.Printf("Error saving final status for job %s: %v", job.ID, err)
	}
}
