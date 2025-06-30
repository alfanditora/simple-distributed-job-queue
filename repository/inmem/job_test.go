package inmemrepo

import (
	"context"
	"jobqueue/entity"
	"testing"
)

func TestJobRepository_SaveAndFindByID(t *testing.T) {
	repo := NewJobRepository().Build()
	job := &entity.Job{ID: "id1", Task: "task1", Status: "pending", Token: "token1"}
	err := repo.Save(context.Background(), job)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	found, err := repo.FindByID(context.Background(), "id1")
	if err != nil {
		t.Fatalf("FindByID failed: %v", err)
	}
	if found.ID != job.ID || found.Task != job.Task {
		t.Errorf("Job mismatch: got %+v, want %+v", found, job)
	}
}

func TestJobRepository_FindAll(t *testing.T) {
	repo := NewJobRepository().Build()
	_ = repo.Save(context.Background(), &entity.Job{ID: "id1", Task: "task1", Status: "pending"})
	_ = repo.Save(context.Background(), &entity.Job{ID: "id2", Task: "task2", Status: "completed"})
	jobs, err := repo.FindAll(context.Background())
	if err != nil {
		t.Fatalf("FindAll failed: %v", err)
	}
	if len(jobs) != 2 {
		t.Errorf("expected 2 jobs, got %d", len(jobs))
	}
}

func TestJobRepository_FindByToken(t *testing.T) {
	repo := NewJobRepository().Build()
	job := &entity.Job{ID: "id1", Task: "task1", Status: "pending", Token: "token1"}
	_ = repo.Save(context.Background(), job)
	found, err := repo.FindByToken(context.Background(), "token1")
	if err != nil {
		t.Fatalf("FindByToken failed: %v", err)
	}
	if found == nil || found.ID != job.ID {
		t.Errorf("expected job with ID %s, got %+v", job.ID, found)
	}
}

func TestJobRepository_FindByID_NotFound(t *testing.T) {
	repo := NewJobRepository().Build()
	_, err := repo.FindByID(context.Background(), "notfound")
	if err == nil {
		t.Error("expected error for not found job, got nil")
	}
}

func TestJobRepository_FindByToken_NotFound(t *testing.T) {
	repo := NewJobRepository().Build()
	found, err := repo.FindByToken(context.Background(), "notfound")
	if err != nil {
		t.Fatalf("FindByToken should not error if not found, got: %v", err)
	}
	if found != nil {
		t.Errorf("expected nil for not found token, got %+v", found)
	}
}
