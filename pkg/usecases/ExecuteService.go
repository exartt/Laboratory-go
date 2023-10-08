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
	runtime.GOMAXPROCS(UsedThread)
	var wg sync.WaitGroup

	tempFiles, _ := es.FileService.CreateBuckets(filePath)
	processedFiles := make(chan ProcessedFile, 23)
	memoryUsed := make(chan int64, 69)
	executionTimeR := make(chan int64, 23)
	executionTimeW := make(chan int64, 23)
	idleTimes := make(chan int64, 23)
	tempFilesChan := make(chan string, len(tempFiles))
	isValid := make(chan bool, 1)

	isValid <- true

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

		go func(file string) {
			es.processFile(&wg, file, processedFiles, memoryUsed, executionTimeR, executionTimeW, isValid)
		}(tempFile)
	}
	wg.Wait()

	executionTime := time.Since(startTime).Milliseconds()
	close(memoryUsed)
	close(executionTimeR)
	close(executionTimeW)
	close(idleTimes)
	close(processedFiles)
	close(isValid)

	for path := range processedFiles {
		if !hasThousandLines(path.FileName, path.FileSize) {
			isValid <- false
		}
		deleteFile(path.FileName)
	}

	return entities.ExecutionResult{
		MemoryUsed:     memoryUsed,
		IdleTimes:      idleTimes,
		ExecutionTime:  executionTime,
		ExecutionTimeR: executionTimeR,
		ExecutionTimeW: executionTimeW,
		IsValid:        isValid,
	}
}

func (es *ExecuteService) processFile(wg *sync.WaitGroup, tempFile string,
	processedFiles chan ProcessedFile,
	memoryUsed,
	executionTimeR,
	executionTimeW chan int64,
	isValid chan bool) {

	defer wg.Done()

	var wgInside sync.WaitGroup
	wgInside.Add(3)

	initialMemory := getMemoryNow()

	readChan := make(chan string, 22)
	processChan := make(chan []entities.ProfessionalSalary, 22)
	writeChan := make(chan []entities.ProfessionalSalary, 22)

	go es.reader(readChan, processChan, wg, executionTimeR, memoryUsed, initialMemory)
	go es.processor(processChan, writeChan, wg, memoryUsed)
	go es.writer(writeChan, wg, executionTimeW, memoryUsed, processedFiles, isValid)

	readChan <- tempFile
	close(readChan)

	wgInside.Wait()
	close(processChan)
	close(writeChan)

	memoryUsed <- getUsedMemory(initialMemory)

	deleteFile(tempFile)
}

func (es *ExecuteService) reader(in chan string, out chan []entities.ProfessionalSalary, wg *sync.WaitGroup, executionTimeR, memoryUsed chan int64, initialMemory uint64) {
	getTime := time.Now()
	for file := range in {
		data, _ := es.FileService.Read(file)
		out <- data
	}
	close(out)
	wg.Done()
	executionTimeR <- time.Now().Sub(getTime).Milliseconds()
	memoryUsed <- getUsedMemory(initialMemory)
}

type ProcessedFile struct {
	FileName string
	FileSize int
}

func (es *ExecuteService) processor(in chan []entities.ProfessionalSalary, out chan []entities.ProfessionalSalary, wg *sync.WaitGroup, memoryUsed chan int64) {
	for data := range in {
		for i := range data {
			titleHash, _ := es.MappingService.GetHash(data[i].JobTitle, enum.TITLE)
			locationHash, _ := es.MappingService.GetHash(data[i].Location, enum.LOCATION)
			data[i].JobTitle = strconv.Itoa(titleHash)
			data[i].Location = strconv.Itoa(locationHash)
		}
		out <- data
	}
	close(out)
	wg.Done()
}

func (es *ExecuteService) writer(
	in chan []entities.ProfessionalSalary,
	wg *sync.WaitGroup,
	executionTimeW, memoryUsed chan int64,
	processedFiles chan ProcessedFile,
	isValid chan bool) {

	defer wg.Done()
	initialMemory := getMemoryNow()
	for data := range in {
		getTime := time.Now()
		result, _ := es.FileService.Write(data)
		executionTimeW <- time.Now().Sub(getTime).Milliseconds()
		memoryUsed <- getUsedMemory(initialMemory)

		if !hasThousandLines(result, len(data)+1) {
			isValid <- false
		}
		processedFiles <- ProcessedFile{FileName: result, FileSize: len(data)}
	}
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
