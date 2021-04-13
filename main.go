package main

import (
	"bufio"
	"encoding/csv"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
)

type Names struct {
	Id    string
	First string
	Last  string
}

type Deets struct {
	Id    string
	Phone string
	Email string
}
type NamesObj struct {
	NameMap map[string]Names
	mut     sync.Mutex
}

type DeetsObj struct {
	DeetsMap map[string]Deets
	mut      sync.Mutex
}

// Go function that digests a file, dumps the appropriate data onto the appropriate channel
func FileDigest(wg *sync.WaitGroup, ec chan error, nc chan Names, dc chan Deets, filename string) {

	defer wg.Done()

	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		ec <- err
		return
	}

	reader := csv.NewReader(bufio.NewReader(file))
	reader.FieldsPerRecord = 3

	title, err := reader.Read() // Grab the first row
	if err != nil {
		log.Fatal(err)
	}

	// Dump onto the channels
	if title[1] == "FirstName" { // Assuming column order
		var name Names

		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			} else {
				name.Id = record[0]
				name.First = record[1]
				name.Last = record[2]

				nc <- name
			}
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	if title[1] == "Phone" { //Again, assuming column order
		var deets Deets
		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			} else {
				deets.Id = record[0]
				deets.Phone = record[1]
				deets.Email = record[2]

				dc <- deets
			}
			if err != nil {
				log.Fatal(err)
			}

		}
	}
}

func Monitor(wg *sync.WaitGroup, ec chan error, nc chan Names, dc chan Deets, done chan bool) {
	wg.Wait()
	close(ec)
	close(nc)
	close(dc)
	done <- true
}

func main() {

	// Get all the files in the directory
	var files []string
	err := filepath.Walk("./data", func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		log.Println(err)
	}

	// Lots of concurrency
	nameChan := make(chan Names)
	deetsChan := make(chan Deets)
	errChan := make(chan error)
	done := make(chan bool)
	wg := new(sync.WaitGroup)

	for _, filename := range files {
		wg.Add(1)
		go FileDigest(wg, errChan, nameChan, deetsChan, filename)
	}

	go Monitor(wg, errChan, nameChan, deetsChan, done)

	do := DeetsObj{DeetsMap: make(map[string]Deets)}
	no := NamesObj{NameMap: make(map[string]Names)}

	go func() {
		for i := range errChan {
			log.Println(i)
		}
	}()

	// Writer to the file
	//f, err := os.Create("./data/new.csv")
	//defer f.Close()
	//if err != nil {
	//	log.Fatalf("file creation error: %s\n", err)
	//}
	//w := csv.NewWriter(f)
	//w.Flush()

	// Throw things onto our maps
	// These should be a function, but I'm doing this for free
	go func() {
		for i := range deetsChan {
			if _, found := no.NameMap[i.Id]; !found {
				do.mut.Lock()
				do.DeetsMap[i.Id] = i
				do.mut.Unlock()
			} else {
				log.Printf("%s %s %s %s %s\n", i.Id, no.NameMap[i.Id].First, no.NameMap[i.Id].Last, i.Email, i.Phone)
			}
		}
	}()

	go func() {
		for i := range nameChan {
			if _, found := do.DeetsMap[i.Id]; !found {
				no.mut.Lock()
				no.NameMap[i.Id] = i
				no.mut.Unlock()
			} else {
				log.Printf("%s %s %s %s %s\n", i.Id, i.First, i.Last, do.DeetsMap[i.Id].Email, do.DeetsMap[i.Id].Phone)
			}
		}
	}()

	<-done
	println(len(no.NameMap))
	println(len(do.DeetsMap))
	// Double Check
	for i, j := range no.NameMap {

		println("---", i)
		if _, found := do.DeetsMap[i]; found {
			log.Printf("%s %s %s %s %s\n", j.Id, j.First, j.Last, do.DeetsMap[i].Email, do.DeetsMap[i].Phone)
			delete(no.NameMap, i)
			delete(do.DeetsMap, i)
		}

	}
	println(len(no.NameMap))
	println(len(do.DeetsMap))
	// Dump the rest
	//for _, j := range do.DeetsMap {
	//	log.Printf("%s %s %s %s %s\n", j.Id, "_", "_", j.Email, j.Phone)
	//}
}
