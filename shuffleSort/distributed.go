package shuffleSort

import (
	"hash/fnv"
	"map-reduce/common"
)

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

/** TODO: DoMapDistributed and DoReduceDistributed
* You will use goroutines to distribute the tasks(you can use channels or waitgroups)
* and use DoMapSequential and DoReduceSequential as a base to this functions
*
 */
func DoMapDistributed(jobName string,
	files []string,
	numberOfMapOutput int,
	mapF func(file string, contents string) []common.KeyValue,
	path string,
	column *string) {
}

func DoReduceDistributed(
	jobName string,
	numberOfMapOutput int,
	numberOfFiles int,
	reduceF func(key string, values []string) string,
	path string) {

}
