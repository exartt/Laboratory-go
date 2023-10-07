package entities

type DataCollected struct {
	Memory               int64
	ExeR                 int64
	ExeW                 int64
	Speedup              float64
	Efficiency           float64
	ExecutionTime        int64
	OverHead             int64
	IdleThreadTimeMedian int64
	IsSingleThread       bool
	FullExecutionTime    int64
	IsValid              bool
}
