package entities

type ExecutionResult struct {
	MemoryUsed    []int64
	IdleTimes     []int64
	ExecutionTime int64
}
