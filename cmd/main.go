package main

import (
	"Laboratory-go/pkg/usecases"
	"Laboratory-go/pkg/utils"
	_ "net/http/pprof"
)

func main() {
	fileService := usecases.NewFileService()
	mappingService := usecases.NewMappingService()

	usecases.DeleteFromGData()
	usecases.DeleteFromGDataOT()

	const repeatProcess = 1000
	//for numGoRoutines := 1; numGoRoutines <= 4; numGoRoutines++ {
	//	fmt.Printf(" ============ ALLOWED GO ROUTINES: %d ============\n", numGoRoutines)
	typeThread := "singleThread"
	utils.SetUsedThread(1)
	utils.PersistDataUsed()
	executeService := usecases.NewExecuteService(fileService, mappingService)
	utils.ExecuteAndCollectData(executeService, typeThread, repeatProcess)

	utils.SetSequentialExecutionTime()
	typeThread = "multiThread"
	utils.SetUsedThread(20)
	utils.PersistDataUsed()
	executeService = usecases.NewExecuteService(fileService, mappingService)

	utils.ExecuteAndCollectData(executeService, typeThread, repeatProcess)
}
