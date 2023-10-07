package main

import (
	"Laboratory-go/pkg/usecases"
	"Laboratory-go/pkg/utils"
	"fmt"
	_ "net/http/pprof"
	"runtime"
)

func main() {
	fileService := usecases.NewFileService()
	mappingService := usecases.NewMappingService()

	usecases.DeleteFromGData()
	usecases.DeleteFromGDataOT()

	const repeatProcess = 50
	for numGoRoutines := 1; numGoRoutines <= 4; numGoRoutines++ {
		fmt.Printf(" ============ ALLOWED GO ROUTINES: %d ============\n", numGoRoutines)
		typeThread := "singleThread"
		runtime.GC()
		if numGoRoutines > 1 {
			utils.SetSequentialExecutionTime()
			typeThread = "multiThread"
		}

		utils.SetUsedThread(numGoRoutines)
		utils.PersistDataUsed()
		executeService := usecases.NewExecuteService(fileService, mappingService)

		utils.ExecuteAndCollectData(executeService, typeThread, repeatProcess)
	}
}
