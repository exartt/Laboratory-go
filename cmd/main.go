package main

import (
	"Laboratory-go/pkg/usecases"
	"Laboratory-go/pkg/utils"
	_ "net/http/pprof"
	"runtime"
)

func main() {
	fileService := usecases.NewFileService()
	mappingService := usecases.NewMappingService()

	usecases.DeleteFromGData()
	usecases.DeleteFromGDataOT()

	const repeatProcess = 1000
	typeThread := "singleThread"
	runtime.GC()
	utils.SetUsedThread(1)
	runtime.GOMAXPROCS(1)
	utils.PersistDataUsed()
	executeService := usecases.NewExecuteService(fileService, mappingService)

	utils.ExecuteAndCollectData(executeService, typeThread, repeatProcess)

	utils.SetSequentialExecutionTime()
	typeThread = "multiThread"
	runtime.GC()
	utils.SetUsedThread(1000)
	runtime.GOMAXPROCS(runtime.NumCPU())
	utils.PersistDataUsed()
	executeService = usecases.NewExecuteService(fileService, mappingService)

	utils.ExecuteAndCollectData(executeService, typeThread, repeatProcess)
}
