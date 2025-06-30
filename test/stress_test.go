package test

import (
	"context"
	"jobqueue/entity"
	"jobqueue/pkg/constant"
	inmemrepo "jobqueue/repository/inmem"
	"jobqueue/service"
	"jobqueue/worker"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestJobWorker_Stress(t *testing.T) {
	log.SetOutput(os.Stdout)

	inMemDb := make(map[string]*entity.Job)
	repo := inmemrepo.NewJobRepository().SetInMemConnection(inMemDb).Build()

	jobService := service.NewJobService().SetJobRepository(repo).Build()

	nJobs := 100

	log.Printf("Preparing %d jobs in the repository...", nJobs)
	for i := 0; i < nJobs; i++ {
		jobID := "job" + strconv.Itoa(i)
		taskName := "normal-job"
		if i%5 == 0 {
			taskName = "unstable-job"
		}

		_, err := jobService.Enqueue(context.Background(), taskName, "idempotency-key-"+jobID)
		if err != nil {
			t.Fatalf("Failed to enqueue job %s: %v", jobID, err)
		}
	}
	log.Printf("All %d jobs initialized as pending.", nJobs)

	workerOptions := constant.WorkerOptions{
		MaxWorkers:   50,
		PollInterval: 10 * time.Millisecond,
		MaxAttempts:  3,
		RetryDelay:   20 * time.Millisecond,
	}
	w := worker.NewJobWorker(repo, workerOptions)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	log.Println("Starting JobWorker for stress test...")
	w.Start(ctx)

	log.Println("Waiting for all jobs to be processed by the worker...")
	pollingInterval := 100 * time.Millisecond
	ticker := time.NewTicker(pollingInterval)
	defer ticker.Stop()

	completedCount := int32(0)
	failedCount := int32(0)

	for {
		select {
		case <-ctx.Done():
			t.Errorf("Test context cancelled or timed out before all jobs completed. %v", ctx.Err())
			goto EndTest
		case <-ticker.C:
			status, err := jobService.GetAllJobStatus(context.Background())
			if err != nil {
				t.Errorf("Error getting job status: %v", err)
				continue
			}

			completedCount = status.Completed
			failedCount = status.Failed
			totalProcessed := completedCount + failedCount
			if totalProcessed == int32(nJobs) {
				log.Println("All jobs reported as processed!")
				goto EndTest
			}
		}
	}

EndTest:
	log.Println("Stopping JobWorker...")
	w.Stop()
	log.Println("JobWorker stopped.")

	finalJobs, err := repo.FindAll(context.Background())
	assert.NoError(t, err, "Should be able to retrieve all jobs at the end")

	actualCompleted := 0
	actualFailed := 0
	actualPending := 0
	actualRunning := 0

	for _, job := range finalJobs {
		switch job.Status {
		case "completed":
			actualCompleted++
		case "failed":
			actualFailed++
		case "pending":
			actualPending++
		case "running":
			actualRunning++
		}
	}

	assert.Equal(t, nJobs, actualCompleted+actualFailed, "Total processed jobs (completed + failed) should match total enqueued jobs")
	assert.Equal(t, 0, actualPending, "No jobs should be pending at the end of the test")
	assert.Equal(t, 0, actualRunning, "No jobs should be running at the end of the test")

	log.Printf("Stress test finished. Total jobs: %d, Completed: %d, Failed: %d", nJobs, actualCompleted, actualFailed)
}
