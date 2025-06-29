package mutation

import (
	"context"
	_dataloader "jobqueue/delivery/graphql/dataloader"
	"jobqueue/delivery/graphql/resolver"
	_interface "jobqueue/interface"
	"log"
)

type JobMutation struct {
	jobService _interface.JobService
	dataloader *_dataloader.GeneralDataloader
}

func (q JobMutation) Enqueue(ctx context.Context, args struct {
	Task  string
	Token *string
}) (*resolver.JobResolver, error) {

	log.Printf("GraphQL Enqueue called with task: %s\n", args.Task)
	token := ""
	if args.Token != nil {
		token = *args.Token
	}
	createdJobID, err := q.jobService.Enqueue(ctx, args.Task, token)
	if err != nil {
		log.Printf("Error in JobMutation.Enqueue: %v\n", err)
		return nil, err
	}

	job, err := q.jobService.GetJobById(ctx, createdJobID)
	if err != nil {
		log.Printf("Error fetching job by ID after enqueue: %v\n", err)
		return nil, err
	}

	return &resolver.JobResolver{
		Data:       *job,
		JobService: q.jobService,
		Dataloader: q.dataloader,
	}, nil
}

// NewJobMutation to create new instance
func NewJobMutation(jobService _interface.JobService, dataloader *_dataloader.GeneralDataloader) JobMutation {
	return JobMutation{
		jobService: jobService,
		dataloader: dataloader,
	}
}
