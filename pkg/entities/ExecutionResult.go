package entities

type ExecutionResult struct {
	MemoryUsed     chan int64
	ExecutionTimeR chan int64
	ExecutionTimeW chan int64
	IdleTimes      chan int64
	ExecutionTime  int64
	IsValid        chan bool
}
