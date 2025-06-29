package entity

type Job struct {
	ID       string `json:"id"`
	Token    string `json:"token,omitempty"`
	Task     string `json:"task"`
	Status   string `json:"status"`
	Attempts int32  `json:"attempts"`
}

type JobStatus struct {
	Pending   int32 `json:"pending"`
	Running   int32 `json:"running"`
	Failed    int32 `json:"failed"`
	Completed int32 `json:"completed"`
}
