package common

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

func MapOutputName(jobName string, mapTask int, reduceTask int) string {
	return jobName + "-mapOutput-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

func ReduceOutputName(jobName string, reduceTask int) string {
	return jobName + "-reduceOutput-" + strconv.Itoa(reduceTask)
}
func ResultName(jobName string) string {
	return jobName + "-result.txt"
}
func MergeAlphabeticalOrder(numberOfMapOutput int, jobName string) {
	fmt.Println("Merge phase")
	kvs := make(map[string]string)
	for i := 0; i < numberOfMapOutput; i++ {
		p := ReduceOutputName(jobName, i)

		file, err := os.Open(p)
		if err != nil {
			log.Fatal("Merge: ", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			kvs[kv.Key] = kv.Value
		}
		file.Close()
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	file, err := os.Create(ResultName(jobName))
	if err != nil {
		log.Fatal("Merge: create ", err)
	}
	w := bufio.NewWriter(file)
	for _, k := range keys {
		fmt.Fprintf(w, "%s: %s\n", k, kvs[k])
	}
	w.Flush()
	file.Close()
}

// most occurrence to least occurrence
func Merge0rderByOccurrence(numberOfMapOutput int, jobName string) {
	//Modify MergeAlphabeticalOrder to show the most frequent word to the least frequent

}

func OpenFiles(column *string) []string {
	var files []string

	if column == nil {
		//root := "/path/to/mapReduceLocal/machado-txt/"
		root := "/path/to/mapReduceLocal/teste/"
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			files = append(files, path)
			return nil
		})
		if err != nil {
			fmt.Println("error reading input files")
			return nil
		}
		files = files[1:]
		return files
	} else {
		inFile := "/path/to/mapReduceLocal/netflix/netflix_titles.csv"
		files := append(files, inFile)
		return files

	}

}
