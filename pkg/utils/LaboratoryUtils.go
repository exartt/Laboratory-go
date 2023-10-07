package utils

import (
	"Laboratory-go/pkg/entities"
	"Laboratory-go/pkg/usecases"
	"bufio"
	"fmt"
	_ "github.com/lib/pq"
	"math/big"
	"os"
	"time"
)

var sequentialExecutionTime float64 = 0

func getMedianMemory(memoryUsedList chan int64) int64 {
	var ret = big.NewInt(0)
	size := int64(len(memoryUsedList))
	for memUsed := range memoryUsedList {
		ret.Add(ret, big.NewInt(memUsed))
	}

	ret.Div(ret, big.NewInt(size))

	return ret.Int64()
}

func PersistData(executionTime, memoryUsedMedian, exeR, exeW, idleThreadTimeMedian int64, isSingleThread bool, fullExecutionTime int64) {
	dataCollected := entities.DataCollected{}

	dataCollected.ExecutionTime = executionTime
	dataCollected.Memory = memoryUsedMedian
	dataCollected.ExeR = exeR
	dataCollected.ExeW = exeW
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
		result := executeService.Execute()

		memoryResult := getMedianMemory(result.MemoryUsed)
		executionTimeR := sumAll(result.ExecutionTimeR)
		executionTimeW := sumAll(result.ExecutionTimeW)
		idleThreadTime := CalculateAverageIdleTimeInMilliseconds(result.IdleTimes)
		timeSpent := (time.Now().UnixNano() / int64(time.Millisecond)) - currentTimeMillis

		PersistData(result.ExecutionTime, memoryResult, executionTimeR, executionTimeW, idleThreadTime, threadType == "singleThread", timeSpent)

		fmt.Printf("capture %s nº %d collected successfully\n", threadType, i)
	}
}

func sumAll(sumChan chan int64) int64 {
	var ret = big.NewInt(0)
	for sum := range sumChan {
		ret.Add(ret, big.NewInt(sum))
	}
	return ret.Div(ret, big.NewInt(23)).Int64()
}

func HasThousandLines(filePath string, size int) bool {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return false
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineCount := 0
	for scanner.Scan() {
		lineCount++
		if lineCount > size {
			return false
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
		return false
	}

	return lineCount == size
}
