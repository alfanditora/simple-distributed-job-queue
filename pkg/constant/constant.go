package constant

import "time"

type contextKey string

const DataloaderContextKey contextKey = "dataloader"

type WorkerOptions struct {
	PollInterval time.Duration // How often the worker checks for new jobs
	MaxWorkers   int32         // Number of concurrent worker goroutines
	RetryDelay   time.Duration // Delay before retrying a failed job
	MaxAttempts  int32         // Maximum number of attempts for a failed job
}

var WorkerOptionsConfig = WorkerOptions{
	PollInterval: 5 * time.Second,
	MaxWorkers:   5,
	RetryDelay:   5 * time.Second,
	MaxAttempts:  3,
}
