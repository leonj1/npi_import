package main

import (
	"log"
	"os"
	"encoding/csv"
	"io"
	"fmt"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
	"flag"
)

func main() {

	var input = flag.String("file", "", "file the be parsed")
	var userName = flag.String("user", "", "db username")
	var password = flag.String("pass", "", "db password")
	var databaseName = flag.String("db", "", "db name")
	flag.Parse()

	// open connection to db
	connectionString := fmt.Sprintf("%s:%s@/%s", userName, password, databaseName)
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()

	// Open doesn't open a connection. Validate DSN data:
	err = db.Ping()
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	// fields we want to record
	// really just interested in the key, but the value helps the reader know the fieldnames of interest
	allowedFields := make(map[int]string)
	allowedFields[0] = "npi"
	allowedFields[4] = "legal_business_name"
	allowedFields[5] = "last_name"
	allowedFields[6] = "first_name"
	allowedFields[7] = "middle_name"
	allowedFields[8] = "name_prefix"
	allowedFields[10] = "credentials"
	allowedFields[11] = "organization_name"
	allowedFields[20] = "business_address"
	allowedFields[22] = "business_city"
	allowedFields[23] = "business_state"
	allowedFields[24] = "business_zip_code"
	allowedFields[25] = "business_country_code"
	allowedFields[26] = "business_phone"
	allowedFields[27] = "business_fax"
	allowedFields[28] = "practice_address"
	allowedFields[29] = "practice_address_line2"
	allowedFields[30] = "practice_city"
	allowedFields[31] = "practice_state"
	allowedFields[32] = "practice_zip_code"
	allowedFields[33] = "practice_country_code"
	allowedFields[36] = "enumeration_date"
	allowedFields[37] = "last_updated"
	allowedFields[41] = "gender"
	allowedFields[47] = "taxonomy_code"
	allowedFields[48] = "license_number"
	allowedFields[49] = "provider_license_number_state_code"

	// Prepare statement for reading data
	stmtOut, err := db.Prepare("SELECT npi FROM npi WHERE npi = ?")
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	defer stmtOut.Close()

	// Prepare statement for deleting existing row
	stmtDel, err := db.Prepare("DELETE FROM npi WHERE npi = ?")
	if err != nil {
		panic(err.Error())
	}
	defer stmtDel.Close()

	// Prepare statement for inserting data
	stmtIns, err := db.Prepare("INSERT INTO npi (npi, legal_business_name, last_name, first_name, middle_name, name_prefix, credentials, organization_name, business_address, business_city, business_state, business_zip_code, business_country_code, business_phone, business_fax, practice_address, practice_address_line2, practice_city, practice_state, practice_zip_code, practice_country_code, enumeration_date, last_updated, gender, taxonomy_code, license_number, provider_license_number_state_code) VALUES( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )")
	//stmtIns, err := db.Prepare("INSERT INTO npi (npi) VALUES(?)")
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	defer stmtIns.Close() // Close the statement when we leave main() / the program terminates

	// open a file, /tmp/test/txt
	if file, err := os.Open(*input); err == nil {

		// make sure it gets closed
		defer file.Close()

		runningCount := 0

		// create a new scanner and read the file line by line
		r := csv.NewReader(file)
		for {
		//for i := 0; i < 3; i++ {
			record, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal(err)
			}

			//for i := 0; i < len(record); i++ {
			//	if _, ok := allowedFields[i]; ok {
			//		fmt.Printf("%s(%d)", record[i], i)
			//	}
			//}
			npiNumber := record[0]
			var result string

			var er = stmtOut.QueryRow("select count(*) as count from npi where npi=?", npiNumber).Scan(&result)
			_, numErr := strconv.Atoi(record[0])

			if (er != nil && result == "" && record[0] != "0" && numErr == nil){
				_, err = stmtIns.Exec(record[0], record[4], record[5], record[6], record[7], record[8],
					record[10], record[11], record[20], record[22], record[23], record[24], record[25],
					record[26], record[27], record[28], record[29], record[30], record[31], record[32],
					record[33], record[36], record[37], record[41], record[47], record[48], record[49])
				if err != nil {
					panic(err.Error())
				}
				runningCount++
				if runningCount % 10000 == 0 {
					fmt.Printf("%d\n", runningCount)
				}
			}
		}

	} else {
		log.Fatal(err)
	}

}
