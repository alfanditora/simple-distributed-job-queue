package worker

import (
	"context"
	"jobqueue/entity"
	"jobqueue/pkg/constant"
	inmemrepo "jobqueue/repository/inmem"
	"testing"
	"time"
)

func TestJobWorker_ProcessNormalJob(t *testing.T) {
	t.Log("[START] TestJobWorker_ProcessNormalJob")
	repo := inmemrepo.NewJobRepository().Build()
	job := &entity.Job{
		ID:       "job1",
		Task:     "normal-job",
		Status:   "pending",
		Attempts: 0,
	}
	t.Logf("[INFO] Save job: %+v", job)
	_ = repo.Save(context.Background(), job)
	worker := NewJobWorker(repo, constant.WorkerOptions{
		MaxWorkers:   1,
		PollInterval: 10 * time.Millisecond,
		MaxAttempts:  3,
		RetryDelay:   10 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	t.Log("[INFO] Start worker")
	worker.Start(ctx)
	time.Sleep(100 * time.Millisecond)
	worker.Stop()
	updated, _ := repo.FindByID(context.Background(), "job1")
	t.Logf("[RESULT] Updated job: %+v", updated)
	if updated.Status != "completed" {
		t.Errorf("expected job status 'completed', got '%s'", updated.Status)
	}
	t.Log("[END] TestJobWorker_ProcessNormalJob")
}

func TestJobWorker_ProcessUnstableJobWithRetry(t *testing.T) {
	t.Log("[START] TestJobWorker_ProcessUnstableJobWithRetry")
	repo := inmemrepo.NewJobRepository().Build()
	job := &entity.Job{
		ID:       "unstable1",
		Task:     "unstable-job",
		Status:   "pending",
		Attempts: 0,
	}
	t.Logf("[INFO] Save job: %+v", job)
	_ = repo.Save(context.Background(), job)
	worker := NewJobWorker(repo, constant.WorkerOptions{
		MaxWorkers:   1,
		PollInterval: 10 * time.Millisecond,
		MaxAttempts:  3,
		RetryDelay:   10 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	t.Log("[INFO] Start worker")
	worker.Start(ctx)
	time.Sleep(300 * time.Millisecond)
	worker.Stop()
	updated, _ := repo.FindByID(context.Background(), "unstable1")
	t.Logf("[RESULT] Updated job: %+v", updated)
	if updated.Status != "completed" {
		t.Errorf("expected unstable job to complete after retries, got '%s'", updated.Status)
	}
	if updated.Attempts != 2 {
		t.Errorf("expected 2 attempts for unstable job, got %d", updated.Attempts)
	}
	t.Log("[END] TestJobWorker_ProcessUnstableJobWithRetry")
}

func TestJobWorker_ExceedMaxAttempts(t *testing.T) {
	t.Log("[START] TestJobWorker_ExceedMaxAttempts")
	repo := inmemrepo.NewJobRepository().Build()
	job := &entity.Job{
		ID:       "failjob",
		Task:     "unstable-job",
		Status:   "pending",
		Attempts: 0,
	}
	t.Logf("[INFO] Save job: %+v", job)
	_ = repo.Save(context.Background(), job)
	worker := NewJobWorker(repo, constant.WorkerOptions{
		MaxWorkers:   1,
		PollInterval: 10 * time.Millisecond,
		MaxAttempts:  2,
		RetryDelay:   10 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	t.Log("[INFO] Start worker")
	worker.Start(ctx)
	time.Sleep(300 * time.Millisecond)
	worker.Stop()
	updated, _ := repo.FindByID(context.Background(), "failjob")
	t.Logf("[RESULT] Updated job: %+v", updated)
	if updated.Status != "failed" {
		t.Errorf("expected job to fail after exceeding max attempts, got '%s'", updated.Status)
	}
	t.Log("[END] TestJobWorker_ExceedMaxAttempts")
}
