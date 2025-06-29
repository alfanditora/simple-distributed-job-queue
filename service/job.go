package service

import (
	"context"
	"fmt"
	"jobqueue/entity"
	_interface "jobqueue/interface"
	"log"

	"github.com/google/uuid"
)

type jobService struct {
	jobRepo _interface.JobRepository
}

// Initiator ...
type Initiator func(s *jobService) *jobService

func (q jobService) GetAllJobs(ctx context.Context) ([]entity.Job, error) {
	jobs, err := q.jobRepo.FindAll(ctx)
	if err != nil {
		log.Printf("Error getting all jobs: %v", err)
		return nil, err
	}
	result := make([]entity.Job, 0, len(jobs))
	for _, job := range jobs {
		result = append(result, *job)
	}
	log.Printf("Found %d jobs", len(result))
	return result, nil
}

func (q jobService) GetJobById(ctx context.Context, id string) (*entity.Job, error) {
	jobs, err := q.jobRepo.FindByID(ctx, id)

	if err != nil {
		log.Printf("Error getting job by ID %s: %v", id, err)
		return nil, err
	}
	if jobs == nil {
		log.Printf("Job with ID %s not found", id)
		return nil, fmt.Errorf("job with ID %s not found", id)
	}
	log.Printf("Found job with ID %s", id)
	return jobs, nil
}

func (q jobService) GetAllJobStatus(ctx context.Context) (entity.JobStatus, error) {
	job, err := q.jobRepo.FindAll(ctx)
	if err != nil {
		return entity.JobStatus{}, err
	}
	status := entity.JobStatus{}
	for _, job := range job {
		switch job.Status {
		case "pending":
			status.Pending++
		case "running":
			status.Running++
		case "failed":
			status.Failed++
		case "completed":
			status.Completed++
		}
	}
	return status, nil
}

// Helper to generate job IDs (simple random string for demo)
func generateJobID() string {
	return uuid.New().String()
}

func (q jobService) Enqueue(ctx context.Context, taskName string, token string) (string, error) {
	if token != "" {
		existingJob, err := q.jobRepo.FindByToken(ctx, token)
		if err != nil {
			log.Printf("Error checking for existing job with token '%s': %v", token, err)
			return "", err
		}
		if existingJob != nil {
			log.Printf("Job with token '%s' already exists, returning existing job ID %s", token, existingJob.ID)
			return existingJob.ID, nil
		}
	} else {
		log.Printf("No token provided, generating a new job ID")
		token = uuid.New().String()
	}

	job := &entity.Job{
		ID:       generateJobID(),
		Task:     taskName,
		Token:    token,
		Status:   "pending",
		Attempts: 0,
	}
	if err := q.jobRepo.Save(ctx, job); err != nil {
		log.Printf("Error saving job '%s' with token '%s': %v", taskName, token, err)
		return "", err
	}
	log.Printf("Enqueued job '%s' with ID %s and token '%s'", taskName, job.ID, token)
	return job.ID, nil
}

// NewJobService ...
func NewJobService() Initiator {
	return func(s *jobService) *jobService {
		return s
	}
}

// SetJobRepository ...
func (i Initiator) SetJobRepository(jobRepository _interface.JobRepository) Initiator {
	return func(s *jobService) *jobService {
		i(s).jobRepo = jobRepository
		return s
	}
}

// Build ...
func (i Initiator) Build() _interface.JobService {
	return i(&jobService{})
}
