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

func deleteFile(path string) error {
	err := os.Remove(path)
	if err != nil {
		return err
	}
	return nil
}

func (es *ExecuteService) Execute() entities.ExecutionResult {
	var wg sync.WaitGroup
	processedFiles := make(chan string, 23)
	memoryUsed := make(chan int64, 69)
	idleTimes := make(chan int64, 23)

	tempFiles, _ := es.FileService.CreateBuckets(filePath)
	tempFilesChan := make(chan string, len(tempFiles))

	for _, path := range tempFiles {
		tempFilesChan <- path
	}

	startTime := time.Now()

	for i := 0; i < UsedThread; i++ {
		wg.Add(1)
		go processFile(&wg, tempFilesChan, processedFiles, memoryUsed, idleTimes, es)
	}

	wg.Wait()

	executionTime := time.Since(startTime).Milliseconds()

	close(processedFiles)
	close(memoryUsed)
	close(tempFilesChan)

	for path := range processedFiles {
		err := deleteFile(path)
		if err != nil {
			log.Print(err)
		}
	}

	return entities.ExecutionResult{
		MemoryUsed:    memoryUsed,
		IdleTimes:     idleTimes,
		ExecutionTime: executionTime,
	}
}

func processFile(wg *sync.WaitGroup, tempFiles chan string, processedFiles chan string, memoryUsed chan int64, idleTimes chan int64, es *ExecuteService) {
	defer wg.Done()

	for tempFile := range tempFiles {
		goroutineStartTime := time.Now()
		initialMemory := getMemoryNow()

		readTime := time.Now()
		professionalSalaries, err := es.FileService.Read(tempFile)
		if err != nil {
			log.Print("deu ruim ", err)
			continue
		}
		print("\nRead ", readTime.Sub(time.Now()).Milliseconds())

		memoryUsed <- getUsedMemory(initialMemory)

		for _, professionalSalary := range professionalSalaries {
			titleHash, _ := es.MappingService.GetHash(professionalSalary.JobTitle, enum.TITLE)
			locationHash, _ := es.MappingService.GetHash(professionalSalary.Location, enum.LOCATION)
			professionalSalary.JobTitle = strconv.Itoa(titleHash)
			professionalSalary.Location = strconv.Itoa(locationHash)
		}

		memoryUsed <- getUsedMemory(initialMemory)
		readTime = time.Now()
		result, err := es.FileService.Write(professionalSalaries)
		if err != nil {
			log.Print("deu ruim ", err)
			continue
		}
		log.Print("\nWrite ", readTime.Sub(time.Now()).Milliseconds())
		processedFiles <- result
		memoryUsed <- getUsedMemory(initialMemory)

		readTime = time.Now()
		err = deleteFile(tempFile)
		if err != nil {
			log.Print("deu ruim ", err)
			continue
		}
		log.Print("\n DEl ", readTime.Sub(time.Now()).Milliseconds())

		goroutineEndTime := time.Now()
		idleTime := goroutineEndTime.Sub(goroutineStartTime).Milliseconds()

		idleTimes <- idleTime
		if len(tempFiles) == 0 {
			return
		}
	}
}
