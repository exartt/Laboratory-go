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
	"strings"
	"time"
)

const (
	MaxRows = 1000
)

type IFileService interface {
	CreateBuckets(pathFile string) ([]string, error)
	Read(filePath string) ([]entities.ProfessionalSalary, error)
	WritePool(data []entities.ProfessionalSalary, builder *strings.Builder) (string, error)
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
	defer file.Close()

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
		tempFile, err := os.CreateTemp(tempDir, "bucket_*"+strconv.FormatInt(currentTimeMillis+((int64(i)/int64(MaxRows))+int64(i+MaxRows)), 10)+".csv")
		if err != nil {
			return nil, err
		}

		writer := bufio.NewWriter(tempFile)
		for _, line := range partition {
			_, err := fmt.Fprintln(writer, line)
			if err != nil {
				tempFile.Close()
				return nil, err
			}
		}
		err = writer.Flush()
		if err != nil {
			tempFile.Close()
			return nil, err
		}

		err = tempFile.Close()
		if err != nil {
			log.Printf("Failed to close temp file: %v", err)
		}

		tempFiles = append(tempFiles, tempFile.Name())
	}

	return tempFiles, nil
}

func (m *FileService) Read(filePath string) ([]entities.ProfessionalSalary, error) {
	professionalSalaryList := make([]entities.ProfessionalSalary, 0, MaxRows)

	file, _ := os.Open(filePath)
	defer file.Close()

	reader := bufio.NewReader(file)
	csvReader := csv.NewReader(reader)

	_, _ = csvReader.Read()

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}

		rating, _ := strconv.ParseFloat(record[0], 64)
		salary, _ := strconv.ParseFloat(record[3], 64)
		reports, _ := strconv.Atoi(record[4])

		professionalSalaryList = append(professionalSalaryList, entities.ProfessionalSalary{
			Rating:      rating,
			CompanyName: record[1],
			JobTitle:    record[2],
			Salary:      salary,
			Reports:     reports,
			Location:    record[5],
		})
	}

	return professionalSalaryList, nil
}

func (m *FileService) Write(professionalSalaries []entities.ProfessionalSalary) (string, error) {
	tempFile, _ := os.CreateTemp("", "bucket_result_*.csv")
	defer tempFile.Close()

	var builder strings.Builder

	builder.WriteString("Rating;CompanyName;JobTitle;Salary;Reports;Location\n")

	for _, salary := range professionalSalaries {
		builder.WriteString(
			strconv.FormatFloat(salary.Rating, 'f', 6, 64) + ";" +
				salary.CompanyName + ";" +
				salary.JobTitle + ";" +
				strconv.FormatFloat(salary.Salary, 'f', 6, 64) + ";" +
				strconv.Itoa(salary.Reports) + ";" +
				salary.Location + "\n",
		)
	}

	writer := bufio.NewWriter(tempFile)
	writer.WriteString(builder.String())

	writer.Flush()

	return tempFile.Name(), nil
}

func (m *FileService) WritePool(professionalSalaries []entities.ProfessionalSalary, builder *strings.Builder) (string, error) {
	tempFile, err := os.CreateTemp("", "bucket_result_*.csv")
	if err != nil {
		return "", err
	}
	defer tempFile.Close()

	writer := bufio.NewWriter(tempFile)

	builder.WriteString("Rating;CompanyName;JobTitle;Salary;Reports;Location\n")

	for _, salary := range professionalSalaries {
		builder.WriteString(
			strconv.FormatFloat(salary.Rating, 'f', 6, 64) + ";" +
				salary.CompanyName + ";" +
				salary.JobTitle + ";" +
				strconv.FormatFloat(salary.Salary, 'f', 6, 64) + ";" +
				strconv.Itoa(salary.Reports) + ";" +
				salary.Location + "\n",
		)
	}

	_, err = writer.WriteString(builder.String())
	if err != nil {
		return "", err
	}

	err = writer.Flush()
	if err != nil {
		return "", err
	}

	return tempFile.Name(), nil
}
