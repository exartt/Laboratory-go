package entities

type DataCollected struct {
	Memory               int64
	Speedup              float64
	Efficiency           float64
	ExecutionTime        int64
	OverHead             float64
	IdleThreadTimeMedian int64
	IsSingleThread       bool
	FullExecutionTime    int64
}
