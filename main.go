package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/tidwall/buntdb"
)

func main() {
	//	Delete the buntdb file if it exists
	dbname := "zipgeo.db"
	os.RemoveAll(dbname)

	//	Create it again
	sysdb, err := buntdb.Open(dbname)
	if err != nil {
		log.Fatalf("problem opening the zipgeo.db: %s", err)
	}
	defer sysdb.Close()

	//	Create our indexes
	sysdb.CreateIndex("zip", "zip:*", buntdb.IndexString)

	//	Look for the file zip-coordinates.csv and open it
	file, err := os.Open("zip-coordinates.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	//	Read in the entire csv file
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	//	For each line in the CSV file
	for _, record := range records {
		//	Parse
		zip := record[0]
		lat := record[1]
		long := record[2]

		//	Create a new entry in the buntdb
		err = sysdb.Update(func(tx *buntdb.Tx) error {
			_, _, err := tx.Set(GetKey("zip", zip), fmt.Sprintf("%s,%s", lat, long), nil)
			return err
		})

		//	If there was an error saving the data, report it:
		if err != nil {
			log.Fatalf("problem saving the zip/geo data: %s", err)
		}

		//	Print out the value
		fmt.Printf("Saving data for %s\n", zip)
	}

}

// GetKey returns a key to be used in the storage system
func GetKey(entityType string, keyPart ...string) string {
	allparts := []string{}
	allparts = append(allparts, entityType)
	allparts = append(allparts, keyPart...)
	return strings.Join(allparts, ":")
}
