package usecases

import "sync"

type IMappingService interface {
	GetHash(key string, hashType string) (int, error)
}

type MappingService struct {
	mu          sync.RWMutex
	maps        map[string]map[string]int
	generalHash int
}

func NewMappingService() *MappingService {
	return &MappingService{
		maps: make(map[string]map[string]int),
	}
}

func (m *MappingService) GetHash(key string, hashType string) (int, error) {
	m.mu.RLock()
	targetMap, exists := m.maps[hashType]
	val, ok := targetMap[key]
	m.mu.RUnlock()
	if exists && ok {
		return val, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	targetMap, exists = m.maps[hashType]
	if !exists {
		targetMap = make(map[string]int)
		m.maps[hashType] = targetMap
	}

	if val, ok = targetMap[key]; ok {
		return val, nil
	}
	targetMap[key] = m.generalHash
	m.generalHash++
	return targetMap[key], nil
}
