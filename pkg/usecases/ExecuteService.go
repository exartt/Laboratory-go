package usecases

import (
	"Laboratory-go/pkg/entities"
	"Laboratory-go/pkg/usecases/enum"
	"fmt"
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
	log.Printf("1")
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
	log.Printf("2")
	for i := 0; i < UsedThread; i++ {
		log.Printf("3")
		wg.Add(1)
		log.Printf("4")
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
	log.Printf("5")
	defer wg.Done()
	log.Printf("6")
	for tempFile := range tempFiles {
		log.Printf("7")
		fmt.Printf("loops?", tempFile)
		goroutineStartTime := time.Now()
		initialMemory := getMemoryNow()
		log.Printf("8")
		professionalSalaries, err := es.FileService.Read(tempFile)
		if err != nil {
			log.Print("deu ruim ", err)
			continue
		}

		log.Printf("9")
		memoryUsed <- getUsedMemory(initialMemory)

		for _, professionalSalary := range professionalSalaries {
			titleHash, _ := es.MappingService.GetHash(professionalSalary.JobTitle, enum.TITLE)
			locationHash, _ := es.MappingService.GetHash(professionalSalary.Location, enum.LOCATION)
			professionalSalary.JobTitle = strconv.Itoa(titleHash)
			professionalSalary.Location = strconv.Itoa(locationHash)
		}
		log.Printf("10")
		memoryUsed <- getUsedMemory(initialMemory)
		result, err := es.FileService.Write(professionalSalaries)
		if err != nil {
			log.Print("deu ruim ", err)
			continue
		}
		processedFiles <- result
		memoryUsed <- getUsedMemory(initialMemory)
		log.Printf("11")
		err = deleteFile(tempFile)
		if err != nil {
			log.Print("deu ruim ", err)
			continue
		}
		log.Printf("12")
		goroutineEndTime := time.Now()
		idleTime := goroutineEndTime.Sub(goroutineStartTime).Milliseconds()
		log.Printf("13")
		idleTimes <- idleTime
		if len(tempFiles) == 0 {
			return
		}
		fmt.Printf("later????")
	}
}
