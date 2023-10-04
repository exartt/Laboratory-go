package entities

type ExecutionResult struct {
	MemoryUsed    chan int64
	MemoryUsedR   chan int64
	MemoryUsedW   chan int64
	IdleTimes     chan int64
	ExecutionTime int64
}
