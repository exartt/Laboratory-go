package main

import (
	"Laboratory-go/pkg/usecases"
	"Laboratory-go/pkg/utils"
)

func main() {
	fileService := usecases.NewFileService()
	mappingService := usecases.NewMappingService()
	executeService := usecases.NewExecuteService(fileService, mappingService)
	const repeatProcess = 5

	utils.ExecuteAndCollectData(executeService, "singleThread", repeatProcess)

	utils.SetSequentialExecutionTime()
	utils.SetUsedThread(10)
	utils.PersistDataUsed()

	executeService = usecases.NewExecuteService(fileService, mappingService)
	utils.ExecuteAndCollectData(executeService, "multiThread", repeatProcess)
}
