package service

import (
	"context"
	"errors"
	inmemrepo "jobqueue/repository/inmem"
	"testing"
)

func setupJobService() *jobService {
	repo := inmemrepo.NewJobRepository().Build()
	return &jobService{jobRepo: repo}
}

func TestEnqueueAndGetJobById(t *testing.T) {
	svc := setupJobService()
	ctx := context.Background()
	jobID, err := svc.Enqueue(ctx, "test-task", "token-123")
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	job, err := svc.GetJobById(ctx, jobID)
	if err != nil {
		t.Fatalf("GetJobById failed: %v", err)
	}
	if job.Task != "test-task" {
		t.Errorf("expected task 'test-task', got '%s'", job.Task)
	}
	if job.Token != "token-123" {
		t.Errorf("expected token 'token-123', got '%s'", job.Token)
	}
}

func TestEnqueue_Idempotency(t *testing.T) {
	svc := setupJobService()
	ctx := context.Background()
	jobID1, err := svc.Enqueue(ctx, "task", "token-abc")
	if err != nil {
		t.Fatalf("first Enqueue failed: %v", err)
	}
	jobID2, err := svc.Enqueue(ctx, "task", "token-abc")
	if err != nil {
		t.Fatalf("second Enqueue failed: %v", err)
	}
	if jobID1 != jobID2 {
		t.Errorf("expected same job ID for same token, got %s and %s", jobID1, jobID2)
	}
}

func TestGetAllJobs(t *testing.T) {
	svc := setupJobService()
	ctx := context.Background()
	_, _ = svc.Enqueue(ctx, "task1", "token1")
	_, _ = svc.Enqueue(ctx, "task2", "token2")
	jobs, err := svc.GetAllJobs(ctx)
	if err != nil {
		t.Fatalf("GetAllJobs failed: %v", err)
	}
	if len(jobs) != 2 {
		t.Errorf("expected 2 jobs, got %d", len(jobs))
	}
}

func TestGetAllJobStatus(t *testing.T) {
	svc := setupJobService()
	ctx := context.Background()
	_, _ = svc.Enqueue(ctx, "task1", "token1")
	_, _ = svc.Enqueue(ctx, "task2", "token2")
	status, err := svc.GetAllJobStatus(ctx)
	if err != nil {
		t.Fatalf("GetAllJobStatus failed: %v", err)
	}
	if status.Pending != 2 {
		t.Errorf("expected 2 pending jobs, got %d", status.Pending)
	}
}

func TestGetJobById_NotFound(t *testing.T) {
	svc := setupJobService()
	ctx := context.Background()
	_, err := svc.GetJobById(ctx, "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent job ID, got nil")
	}
	if !errors.Is(err, errors.New("job not found")) && err.Error() != "job with ID nonexistent not found" {
		t.Errorf("unexpected error: %v", err)
	}
}
