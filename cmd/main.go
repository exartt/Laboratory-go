package main

import (
	"Laboratory-go/pkg/usecases"
	"Laboratory-go/pkg/utils"
)

func main() {
	fileService := usecases.NewFileService()
	mappingService := usecases.NewMappingService()
	const repeatProcess = 10

	for numGoRoutines := 1; numGoRoutines <= 10; numGoRoutines++ {
		typeThread := "singleThread"
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
