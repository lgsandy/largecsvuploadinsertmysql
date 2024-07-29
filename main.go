package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gofiber/fiber/v2"
)

var db *sql.DB

const batchSize = 1000

func initDB() {
	var err error
	dsn := "root:root@tcp(127.0.0.1:3306)/test"
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}

	if err = db.Ping(); err != nil {
		log.Fatal(err)
	}
}

func parseAndInsertCSV(file multipart.File) error {
	t := time.Now()
	reader := csv.NewReader(bufio.NewReader(file))
	recoed, err := reader.Read()
	if err != nil {
		return err
	}
	fmt.Println(recoed)
	var wg sync.WaitGroup
	recordChan := make(chan []string, batchSize)

	go func() {
		defer close(recordChan)
		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Println("Error reading CSV:", err)
				continue
			}
			recordChan <- record
		}
	}()

	for i := 0; i < 10; i++ { // Start 10 workers for concurrent processing
		wg.Add(1)
		go func() {
			defer wg.Done()
			var records [][]string
			for record := range recordChan {
				records = append(records, record)
				if len(records) >= batchSize {
					if err := insertRecords(records); err != nil {
						log.Println("Error inserting records:", err)
					}
					records = nil
				}
			}
			if len(records) > 0 {
				if err := insertRecords(records); err != nil {
					log.Println("Error inserting records:", err)
				}
			}
		}()
	}

	wg.Wait()
	log.Printf("handler took %s", time.Since(t))
	return nil
}

func insertRecords(records [][]string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare("INSERT INTO ward (district, mandal,village,mp,mla,no_of_wards) VALUES (?, ?, ?,?,?,?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, record := range records {
		district := record[0]
		mandal := record[1]
		village := record[2]
		mp := record[3]
		mla := record[4]
		no_of_wards := record[5]
		// fmt.Println("values---" + district + "-" + mandal + "-" + village + "-" + mp + "-" + mla + "-" + no_of_wards)
		if _, err := stmt.Exec(district, mandal, village, mp, mla, no_of_wards); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func uploadHandler(c *fiber.Ctx) error {
	file, err := c.FormFile("file")
	if err != nil {
		return c.Status(http.StatusBadRequest).SendString("No file found")
	}

	f, err := file.Open()
	if err != nil {
		return c.Status(http.StatusInternalServerError).SendString("Failed to open file")
	}
	defer f.Close()

	err = parseAndInsertCSV(f)
	if err != nil {
		return c.Status(http.StatusInternalServerError).SendString("Failed to parse and insert CSV")
	}

	return c.SendString("File uploaded and processed successfully")
}

func main() {
	initDB()
	app := fiber.New()

	app.Get("/", func(c *fiber.Ctx) error {
		return c.SendString("Hello, World!")
	})
	app.Post("/upload", uploadHandler)

	app.Listen(":3000")
}
