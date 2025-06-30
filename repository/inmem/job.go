package inmemrepo

import (
	"context"
	"errors"
	"jobqueue/entity"
	_interface "jobqueue/interface"
	"sync"
)

type jobRepository struct {
	mu         sync.RWMutex
	inMemDb    map[string]*entity.Job
	tokenIndex map[string]string
}

func (r *jobRepository) Save(ctx context.Context, job *entity.Job) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.tokenIndex == nil {
		r.tokenIndex = make(map[string]string)
	}
	if r.inMemDb == nil {
		r.inMemDb = make(map[string]*entity.Job)
	}

	if _, exists := r.inMemDb[job.ID]; !exists && job.Token != "" {
		if existingID, ok := r.tokenIndex[job.Token]; ok {
			return errors.New("idempotency key '" + job.Token + "' already associated with job ID '" + existingID + "'")
		}
		r.tokenIndex[job.Token] = job.ID
	} else if job.Token != "" {
		r.tokenIndex[job.Token] = job.ID
	}

	r.inMemDb[job.ID] = job
	return nil
}

func (r *jobRepository) FindByID(ctx context.Context, id string) (*entity.Job, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	job, ok := r.inMemDb[id]
	if !ok {
		return nil, errors.New("job not found")
	}
	return job, nil
}

func (r *jobRepository) FindAll(ctx context.Context) ([]*entity.Job, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	jobs := make([]*entity.Job, 0, len(r.inMemDb))
	for _, job := range r.inMemDb {
		jobs = append(jobs, job)
	}
	return jobs, nil
}
func (r *jobRepository) FindByToken(ctx context.Context, token string) (*entity.Job, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, job := range r.inMemDb {
		if job.Token == token {
			return job, nil
		}
	}
	return nil, nil
}

// Initiator ...
type Initiator func(s *jobRepository) *jobRepository

// NewJobRepository ...
func NewJobRepository() Initiator {
	return func(q *jobRepository) *jobRepository {
		return q
	}
}

// SetInMemConnection set database client connection
func (i Initiator) SetInMemConnection(inMemDb map[string]*entity.Job) Initiator {
	return func(s *jobRepository) *jobRepository {
		i(s).inMemDb = inMemDb
		return s
	}
}

// Build ...
func (i Initiator) Build() _interface.JobRepository {
	return i(&jobRepository{})
}
