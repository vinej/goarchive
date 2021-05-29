package task

import "context"

type Status string

const (
	Running   Status = "runnning"
	Paused    Status = "paused"
	Cancelled Status = "cancelled"
	Completed Status = "completed"
	Failed    Status = "failed"
	Success   Status = "success"
)

type ITask interface {
	//GetStatus() Status
	Run(ctx context.Context) (Status, error)
	//Stop(ctx context.Context) (Status, error)
	//Start(ctx context.Context) (Status, error)
	//Cancel(ctx context.Context) (Status, error)
}
