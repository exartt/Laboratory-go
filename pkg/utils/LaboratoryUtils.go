package utils

import (
	"Laboratory-go/pkg/entities"
	"Laboratory-go/pkg/usecases"
	"fmt"
	_ "github.com/lib/pq"
	"math/big"
	"time"
)

var sequentialExecutionTime float64 = 0

func GetMedianMemory(memoryUsedList chan int64) int64 {
	var ret = big.NewInt(0)
	size := int64(len(memoryUsedList))

	for memUsed := range memoryUsedList {
		ret.Add(ret, big.NewInt(memUsed))
	}

	ret.Div(ret, big.NewInt(size))

	return ret.Int64()
}

func PersistData(executionTime, memoryUsedMedian, idleThreadTimeMedian int64, isSingleThread bool, fullExecutionTime int64) {
	dataCollected := entities.DataCollected{}

	dataCollected.ExecutionTime = executionTime
	dataCollected.Memory = memoryUsedMedian
	dataCollected.IsSingleThread = isSingleThread

	if !isSingleThread {
		speedUp := GetSpeedup(executionTime)
		efficiency := GetEfficiency(speedUp)
		overHead := GetOverhead(executionTime)
		dataCollected.Speedup = speedUp
		dataCollected.Efficiency = efficiency
		dataCollected.OverHead = overHead
	}
	dataCollected.FullExecutionTime = fullExecutionTime
	dataCollected.IdleThreadTimeMedian = idleThreadTimeMedian

	usecases.Insert(dataCollected)
}

func GetSpeedup(parallelExecutionTime int64) float64 {
	return sequentialExecutionTime / float64(parallelExecutionTime)
}

func GetEfficiency(speedUp float64) float64 {
	return speedUp / float64(usecases.UsedThread)
}

func GetOverhead(parallelExecutionTime int64) int64 {
	return int64(sequentialExecutionTime) - parallelExecutionTime
}

func CalculateAverageIdleTimeInMilliseconds(idleTimes chan int64) int64 {
	var totalIdleTime int64 = 0
	size := int64(len(idleTimes))
	for idleTime := range idleTimes {
		totalIdleTime += idleTime
		if len(idleTimes) == 0 {
			break
		}
	}

	return totalIdleTime / size
}

func SetSequentialExecutionTime() {
	sequentialExecutionTime = usecases.GetAverageExecutionTime()
}

func PersistDataUsed() {
	usecases.InsertData(sequentialExecutionTime, usecases.UsedThread)
}

func SetUsedThread(threads int) {
	usecases.UsedThread = threads
}

func ExecuteAndCollectData(executeService *usecases.ExecuteService, threadType string, numIterations int) {
	for i := 0; i < numIterations; i++ {
		fmt.Printf("Initiating %s capture number: %d\n", threadType, i)

		currentTimeMillis := time.Now().UnixNano() / int64(time.Millisecond)
		fmt.Printf("Where do i die?")
		result := executeService.Execute()

		memoryResult := GetMedianMemory(result.MemoryUsed)
		idleThreadTime := CalculateAverageIdleTimeInMilliseconds(result.IdleTimes)
		timeSpent := (time.Now().UnixNano() / int64(time.Millisecond)) - currentTimeMillis

		PersistData(result.ExecutionTime, memoryResult, idleThreadTime, threadType == "singleThread", timeSpent)

		fmt.Printf("capture %s nº %d collected successfully\n", threadType, i)
	}
}
