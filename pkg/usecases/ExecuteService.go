package usecases

import (
	"Laboratory-go/pkg/entities"
	"Laboratory-go/pkg/usecases/enum"
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

var UsedThread = 1
var IsValid = true

const (
	filePath = "resources/Software_Professional_Salaries.csv"
	//filePath = "/home/opc/Laboratory-go/resources/Software_Professional_Salaries.csv"
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

func deleteFile(path string) error {
	err := os.Remove(path)
	if err != nil {
		return err
	}
	return nil
}

func (es *ExecuteService) Execute() entities.ExecutionResult {
	runtime.GOMAXPROCS(UsedThread)
	IsValid = true
	var wg sync.WaitGroup

	tempFiles, _ := es.FileService.CreateBuckets(filePath)
	processedFiles := make(chan string, 23)
	memoryUsed := make(chan int64, 69)
	executionTimeR := make(chan int64, 23)
	executionTimeW := make(chan int64, 23)
	idleTimes := make(chan int64, 23)
	tempFilesChan := make(chan string, len(tempFiles))

	for _, path := range tempFiles {
		tempFilesChan <- path
	}
	close(tempFilesChan)

	startTime := time.Now()
	for _, tempFile := range tempFiles {
		waitStart := time.Now()
		waitEnd := time.Now()
		waitTime := waitEnd.Sub(waitStart).Milliseconds()
		idleTimes <- waitTime

		wg.Add(1)
		go func(file string) {
			es.processFile(&wg, file, processedFiles, memoryUsed, executionTimeR, executionTimeW)
		}(tempFile)
	}
	wg.Wait()

	executionTime := time.Since(startTime).Milliseconds()
	close(memoryUsed)
	close(executionTimeR)
	close(executionTimeW)
	close(idleTimes)
	close(processedFiles)

	for path := range processedFiles {
		deleteFile(path)
	}

	return entities.ExecutionResult{
		MemoryUsed:     memoryUsed,
		IdleTimes:      idleTimes,
		ExecutionTime:  executionTime,
		ExecutionTimeR: executionTimeR,
		ExecutionTimeW: executionTimeW,
	}
}

func (es *ExecuteService) processFile(wg *sync.WaitGroup, tempFile string,
	processedFiles chan string,
	memoryUsed chan int64,
	executionTimeR chan int64,
	executionTimeW chan int64) {

	defer wg.Done()

	initialMemory := getMemoryNow()

	getTime := time.Now()
	professionalSalaries, _ := es.FileService.Read(tempFile)
	executionTimeR <- time.Now().Sub(getTime).Milliseconds()

	for i := range professionalSalaries {
		titleHash, _ := es.MappingService.GetHash(professionalSalaries[i].JobTitle, enum.TITLE)
		locationHash, _ := es.MappingService.GetHash(professionalSalaries[i].Location, enum.LOCATION)
		professionalSalaries[i].JobTitle = strconv.Itoa(titleHash)
		professionalSalaries[i].Location = strconv.Itoa(locationHash)
	}
	memoryUsed <- getUsedMemory(initialMemory)

	getTime = time.Now()
	result, _ := es.FileService.Write(professionalSalaries)
	executionTimeW <- time.Now().Sub(getTime).Milliseconds()

	if !hasThousandLines(result, len(professionalSalaries)+1) {
		IsValid = false
	}
	processedFiles <- result
	memoryUsed <- getUsedMemory(initialMemory)

	deleteFile(tempFile)
}

func hasThousandLines(filePath string, size int) bool {
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
