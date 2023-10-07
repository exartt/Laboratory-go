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
	//filePath = "resources/Software_Professional_Salaries.csv"
	filePath = "/home/opc/Laboratory-go/resources/Software_Professional_Salaries.csv"
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
	var wg sync.WaitGroup
	runtime.GOMAXPROCS(UsedThread)

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

	//for path := range processedFiles {
	//	err := deleteFile(path)
	//	if err != nil {
	//		log.Print(err)
	//	}
	//}

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
	professionalSalaries, err := es.FileService.Read(tempFile)
	if err != nil {
		log.Print("Error while reading ", err)
		return
	}

	executionTimeR <- time.Now().Sub(getTime).Milliseconds()

	for i := range professionalSalaries {
		titleHash, _ := es.MappingService.GetHash(professionalSalaries[i].JobTitle, enum.TITLE)
		locationHash, _ := es.MappingService.GetHash(professionalSalaries[i].Location, enum.LOCATION)
		professionalSalaries[i].JobTitle = strconv.Itoa(titleHash)
		professionalSalaries[i].Location = strconv.Itoa(locationHash)
	}
	memoryUsed <- getUsedMemory(initialMemory)

	getTime = time.Now()
	result, err := es.FileService.Write(professionalSalaries)
	if err != nil {
		log.Print("Error trying to write ", err)
		return
	}
	executionTimeW <- time.Now().Sub(getTime).Milliseconds()

	processedFiles <- result
	memoryUsed <- getUsedMemory(initialMemory)

	err = deleteFile(tempFile)
	if err != nil {
		log.Print("Error trying to delete single file ", err)
		return
	}
}
