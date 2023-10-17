package usecases

import (
	"Laboratory-go/pkg/entities"
	"Laboratory-go/pkg/usecases/enum"
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

var pool = &sync.Pool{
	New: func() interface{} {
		return new(strings.Builder)
	},
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

	//tempFiles, _ := es.FileService.CreateBuckets(filePath)
	//processedFiles := make(chan string, 23)
	//memoryUsed := make(chan int64, 69)
	//executionTimeR := make(chan int64, 23)
	//executionTimeW := make(chan int64, 23)
	//idleTimes := make(chan int64, 23)
	//tempFilesChan := make(chan string, len(tempFiles))
	//isValid := make(chan bool, 1)

	var tempFiles []string
	for i := 0; i < 50; i++ {
		files, err := es.FileService.CreateBuckets(filePath)
		if err != nil {
			fmt.Println("Erro ao criar buckets:", err)
			panic("error")
		}
		tempFiles = append(tempFiles, files...)
	}
	processedFiles := make(chan string, len(tempFiles))
	memoryUsed := make(chan int64, 3*len(tempFiles))
	executionTimeR := make(chan int64, len(tempFiles))
	executionTimeW := make(chan int64, len(tempFiles))
	idleTimes := make(chan int64, len(tempFiles))
	tempFilesChan := make(chan string, len(tempFiles))
	isValid := make(chan bool, 1)

	var lastEndTime int64

	isValid <- true

	for _, path := range tempFiles {
		tempFilesChan <- path
	}
	close(tempFilesChan)

	pool := &WorkerPool{
		maxWorkers:  UsedThread,
		queuedTasks: make(chan func(), len(tempFiles)),
	}

	pool.Run()

	startTime := time.Now()
	for _, tempFile := range tempFiles {
		waitTime := time.Now().Sub(time.Unix(0, atomic.LoadInt64(&lastEndTime))).Milliseconds()
		idleTimes <- waitTime

		wg.Add(1)
		localFile := tempFile
		pool.AddTask(func() {
			defer wg.Done()
			es.processFile(localFile, processedFiles, memoryUsed, executionTimeR, executionTimeW, isValid)
			atomic.StoreInt64(&lastEndTime, time.Now().UnixMilli())
		})
	}

	wg.Wait()
	close(memoryUsed)
	close(executionTimeR)
	close(executionTimeW)
	close(idleTimes)
	close(processedFiles)
	close(isValid)

	executionTime := time.Since(startTime).Milliseconds()

	for path := range processedFiles {
		deleteFile(path)
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

func (es *ExecuteService) processFile(tempFile string,
	processedFiles chan string,
	memoryUsed,
	executionTimeR,
	executionTimeW chan int64,
	isValid chan bool) {

	initialMemory := getMemoryNow()

	builder := pool.Get().(*strings.Builder)
	defer pool.Put(builder)
	builder.Reset()

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
	result, _ := es.FileService.Write(professionalSalaries, builder)
	executionTimeW <- time.Now().Sub(getTime).Milliseconds()

	if !hasThousandLines(result, len(professionalSalaries)+1) {
		isValid <- false
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

// WORKER POOL // TODO -> aplicar clean code eventualmente, realocando as funções

type WorkerPool struct {
	maxWorkers  int
	queuedTasks chan func()
}

func (wp *WorkerPool) Run() {
	for i := 0; i < wp.maxWorkers; i++ {
		go func() {
			for task := range wp.queuedTasks {
				task()
			}
		}()
	}
}

func (wp *WorkerPool) AddTask(task func()) {
	wp.queuedTasks <- task
}
