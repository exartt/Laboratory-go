package usecases

import (
	"Laboratory-go/pkg/entities"
	"Laboratory-go/pkg/usecases/enum"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

var UsedThread = 1

const (
	filePath = "resources/Software_Professional_Salaries.csv"
)

type IExecuteService interface {
	Execute() (entities.ExecutionResult, error)
}

type ExecuteService struct {
	FileService    IFileService
	MappingService IMappingService
}

func NewExecuteService(fileService IFileService, mappingService IMappingService) *ExecuteService {
	return &ExecuteService{
		FileService:    fileService,
		MappingService: mappingService,
	}
}

func getMemoryNow() uint64 {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	return memStats.Alloc
}

func getUsedMemory(initialMemory uint64) int64 {
	return int64(getMemoryNow() - initialMemory)
}

func deleteFile(path string, mu *sync.Mutex) error {
	mu.Lock()
	defer mu.Unlock()

	err := os.Remove(path)
	if err != nil {
		return err
	}
	return nil
}

func (es *ExecuteService) Execute() entities.ExecutionResult {
	var wg sync.WaitGroup
	processedFiles := make(chan string, 22)
	memoryUsed := make(chan int64, 100)
	var idleTimes []int64
	mu := &sync.Mutex{}

	sem := make(chan struct{}, UsedThread)

	startTime := time.Now()

	tempFiles, _ := es.FileService.CreateBuckets(filePath)

	for i, tempFile := range tempFiles {
		wg.Add(1)
		go func(i int, tempFile string) {
			sem <- struct{}{}
			processFile(&wg, mu, tempFile, processedFiles, memoryUsed, &idleTimes, es)
			<-sem
		}(i, tempFile)
	}

	go closeChannels(&wg, processedFiles, memoryUsed)

	executionTime := time.Since(startTime).Milliseconds()

	totalMemoryUsed := collectMemoryUsed(memoryUsed)

	for path := range processedFiles {
		err := deleteFile(path, mu)
		if err != nil {
			log.Fatal(err)
		}
	}

	return entities.ExecutionResult{
		MemoryUsed:    []int64{totalMemoryUsed},
		IdleTimes:     idleTimes,
		ExecutionTime: executionTime,
	}
}

func processFile(wg *sync.WaitGroup, mu *sync.Mutex, tempFile string, processedFiles chan string, memoryUsed chan int64, idleTimes *[]int64, es *ExecuteService) {
	defer wg.Done()
	goroutineStartTime := time.Now()
	initialMemory := getMemoryNow()

	professionalSalaries, err := es.FileService.Read(tempFile)
	if err != nil {
		log.Fatal(err)
	}
	memoryUsed <- getUsedMemory(initialMemory)

	for _, professionalSalary := range professionalSalaries {
		titleHash, _ := es.MappingService.GetHash(professionalSalary.JobTitle, enum.TITLE)
		locationHash, _ := es.MappingService.GetHash(professionalSalary.Location, enum.LOCATION)
		professionalSalary.JobTitle = strconv.Itoa(titleHash)
		professionalSalary.Location = strconv.Itoa(locationHash)
	}

	result, err := es.FileService.Write(professionalSalaries)
	if err != nil {
		log.Fatal(err)
	}
	processedFiles <- result
	memoryUsed <- getUsedMemory(initialMemory)

	err = deleteFile(tempFile, mu)
	if err != nil {
		return
	}

	goroutineEndTime := time.Now()
	idleTime := goroutineEndTime.Sub(goroutineStartTime).Milliseconds()

	mu.Lock()
	*idleTimes = append(*idleTimes, idleTime)
	mu.Unlock()
}

func closeChannels(wg *sync.WaitGroup, processedFiles chan string, memoryUsed chan int64) {
	wg.Wait()
	close(processedFiles)
	close(memoryUsed)
}

func collectMemoryUsed(memoryUsed chan int64) int64 {
	var totalMemoryUsed int64
	for m := range memoryUsed {
		totalMemoryUsed += m
	}
	return totalMemoryUsed
}
