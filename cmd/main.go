package main

import (
	"Laboratory-go/pkg/usecases"
	"Laboratory-go/pkg/utils"
)

func main() {
	fileService := usecases.NewFileService()
	mappingService := usecases.NewMappingService()
	const repeatProcess = 1

	for numThreads := 1; numThreads <= 10; numThreads++ {
		typeThread := "singleThread"
		if numThreads > 1 {
			utils.SetSequentialExecutionTime()
			typeThread = "multiThread"
		}

		utils.SetUsedThread(numThreads)
		utils.PersistDataUsed()
		executeService := usecases.NewExecuteService(fileService, mappingService)

		utils.ExecuteAndCollectData(executeService, typeThread, repeatProcess)
	}
}
