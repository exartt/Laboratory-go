package usecases

import (
	"Laboratory-go/pkg/entities"
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"
)

const (
	MaxRows = 1000
)

type IFileService interface {
	CreateBuckets(pathFile string) ([]string, error)
	Read(filePath string) ([]entities.ProfessionalSalary, error)
	Write(data []entities.ProfessionalSalary) (string, error)
}

type FileService struct {
}

func NewFileService() *FileService {
	return &FileService{}
}

func (m *FileService) CreateBuckets(pathFile string) ([]string, error) {
	var tempFiles []string
	file, err := os.Open(pathFile)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(file)

	scanner := bufio.NewScanner(file)
	var allLines []string
	for scanner.Scan() {
		allLines = append(allLines, scanner.Text())
	}

	for i := 0; i < len(allLines); i += MaxRows {
		end := i + MaxRows
		if end > len(allLines) {
			end = len(allLines)
		}
		partition := allLines[i:end]

		currentTimeMillis := time.Now().UnixNano() / int64(time.Millisecond)
		tempDir := os.TempDir()
		tempFile, err := os.CreateTemp(tempDir, "bucket_*"+strconv.FormatInt(currentTimeMillis+(int64(i)/int64(MaxRows)), 10)+".csv")
		if err != nil {
			return nil, err
		}

		writer := bufio.NewWriter(tempFile)
		for _, line := range partition {
			_, err := fmt.Fprintln(writer, line)
			if err != nil {
				return nil, err
			}
		}
		err = writer.Flush()
		if err != nil {
			return nil, err
		}

		tempFiles = append(tempFiles, tempFile.Name())
	}

	return tempFiles, nil
}

func (m *FileService) Read(partFilePath string) ([]entities.ProfessionalSalary, error) {
	file, err := os.Open(partFilePath)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	var professionalSalaries []entities.ProfessionalSalary
	reader := csv.NewReader(file)
	_, err = reader.Read()
	if err != nil {
		return nil, err
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		rating, _ := strconv.ParseFloat(record[0], 64)
		salary, _ := strconv.ParseFloat(record[3], 64)
		reports, _ := strconv.Atoi(record[4])

		professionalSalary := entities.ProfessionalSalary{
			Rating:      rating,
			CompanyName: record[1],
			JobTitle:    record[2],
			Salary:      salary,
			Reports:     reports,
			Location:    record[5],
		}

		professionalSalaries = append(professionalSalaries, professionalSalary)
	}
	return professionalSalaries, nil
}

func (m *FileService) Write(professionalSalaries []entities.ProfessionalSalary) (string, error) {
	tempFile, err := os.CreateTemp("", "bucket_result_*.csv")
	if err != nil {
		return "", err
	}
	defer func(tempFile *os.File) {
		err := tempFile.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(tempFile)

	writer := bufio.NewWriter(tempFile)
	_, err = writer.WriteString("Rating,CompanyName,JobTitle,Salary,Reports,Location\n")
	if err != nil {
		return "", err
	}

	for _, salary := range professionalSalaries {
		line := fmt.Sprintf("%f,%s,%s,%f,%d,%s\n",
			salary.Rating,
			salary.CompanyName,
			salary.JobTitle,
			salary.Salary,
			salary.Reports,
			salary.Location)
		_, err := writer.WriteString(line)
		if err != nil {
			return "", err
		}
	}

	err = writer.Flush()
	if err != nil {
		return "", err
	}

	return tempFile.Name(), nil
}
