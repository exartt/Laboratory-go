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
	professionalSalaryList := make([]entities.ProfessionalSalary, 0, 1000)

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("could not open file: %v", err)
	}
	defer func() {
		if cerr := file.Close(); cerr != nil {
			log.Printf("Failed to close file during reading: %v", cerr)
		}
	}()

	bufferedReader := bufio.NewReaderSize(file, 64*1024)
	reader := csv.NewReader(bufferedReader)

	if _, err := reader.Read(); err != nil {
		return nil, fmt.Errorf("error reading header: %v", err)
	}

	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading line: %v", err)
		}

		rating, err := strconv.ParseFloat(line[0], 64)
		if err != nil {
			return nil, fmt.Errorf("could not parse rating: %v", err)
		}
		salary, err := strconv.ParseFloat(line[3], 64)
		if err != nil {
			return nil, fmt.Errorf("could not parse salary: %v", err)
		}
		reports, err := strconv.Atoi(line[4])
		if err != nil {
			return nil, fmt.Errorf("could not parse reports: %v", err)
		}

		professionalSalary := entities.ProfessionalSalary{
			Rating:      rating,
			CompanyName: line[1],
			JobTitle:    line[2],
			Salary:      salary,
			Reports:     reports,
			Location:    line[5],
		}
		professionalSalaryList = append(professionalSalaryList, professionalSalary)
	}

	return professionalSalaryList, nil
}

//func (m *FileService) Read(filePath string) ([]entities.ProfessionalSalary, error) {
//	professionalSalaryList := make([]entities.ProfessionalSalary, 0, 1000)
//
//	file, err := os.Open(filePath)
//	if err != nil {
//		return nil, fmt.Errorf("could not open file: %v", err)
//	}
//	defer func() {
//		err := file.Close()
//		if err != nil {
//			log.Printf("Failed to close file during reading: %v", err)
//		}
//	}()
//
//	reader := csv.NewReader(file)
//	reader.Read()
//
//	for {
//		line, err := reader.Read()
//		if err != nil {
//			break
//		}
//
//		rating, err := strconv.ParseFloat(line[0], 64)
//		if err != nil {
//			return nil, fmt.Errorf("could not parse rating: %v", err)
//		}
//
//		companyName := line[1]
//		jobTitle := line[2]
//
//		salary, err := strconv.ParseFloat(line[3], 64)
//		if err != nil {
//			return nil, fmt.Errorf("could not parse salary: %v", err)
//		}
//
//		reports, err := strconv.Atoi(line[4])
//		if err != nil {
//			return nil, fmt.Errorf("could not parse reports: %v", err)
//		}
//
//		location := line[5]
//
//		professionalSalary := entities.ProfessionalSalary{
//			Rating:      rating,
//			CompanyName: companyName,
//			JobTitle:    jobTitle,
//			Salary:      salary,
//			Reports:     reports,
//			Location:    location,
//		}
//
//		professionalSalaryList = append(professionalSalaryList, professionalSalary)
//	}
//
//	return professionalSalaryList, nil
//}

//func (m *FileService) Write(professionalSalaries []entities.ProfessionalSalary) (string, error) {
//	tempFile, err := os.CreateTemp("", "bucket_result_*.csv")
//	if err != nil {
//		return "", err
//	}
//	defer tempFile.Close()
//
//	writer := bufio.NewWriter(tempFile)
//	_, err = writer.WriteString("Rating;CompanyName;JobTitle;Salary;Reports;Location\n")
//	if err != nil {
//		return "", err
//	}
//
//	var builder strings.Builder
//	for _, salary := range professionalSalaries {
//		fmt.Fprintf(&builder, "%f;%s;%s;%f;%d;%s\n",
//			salary.Rating,
//			salary.CompanyName,
//			salary.JobTitle,
//			salary.Salary,
//			salary.Reports,
//			salary.Location)
//	}
//
//	_, err = writer.WriteString(builder.String())
//	if err != nil {
//		return "", err
//	}
//
//	if err = writer.Flush(); err != nil {
//		return "", err
//	}
//
//	return tempFile.Name(), nil
//}

func (m *FileService) Write(professionalSalaries []entities.ProfessionalSalary) (string, error) {
	tempFile, err := os.CreateTemp("", "bucket_result_*.csv")
	if err != nil {
		return "", err
	}
	defer tempFile.Close()

	writer := bufio.NewWriter(tempFile)

	_, err = writer.WriteString("Rating;CompanyName;JobTitle;Salary;Reports;Location\n")
	if err != nil {
		return "", err
	}

	for _, salary := range professionalSalaries {
		_, err = fmt.Fprintf(writer, "%f;%s;%s;%f;%d;%s\n",
			salary.Rating,
			salary.CompanyName,
			salary.JobTitle,
			salary.Salary,
			salary.Reports,
			salary.Location)
		if err != nil {
			return "", err
		}
	}

	if err = writer.Flush(); err != nil {
		return "", err
	}

	return tempFile.Name(), nil
}
