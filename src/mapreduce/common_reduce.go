package mapreduce

import (
	"os"
	"io"
	//"fmt"
	"encoding/json"
	"io/ioutil"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string,       // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string,       // write the output here
	nMap int,             // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	debug("jobName: %s, reduceTaskNumber: %d, nMap: %d\r\n", jobName, reduceTaskNumber, nMap)
	var rnb []byte = []byte("\r\n")
	var fileList []*os.File

	for i := 0; i < nMap; i++ {
		reduceName := reduceName(jobName, i, reduceTaskNumber)
		reduceFile, _ := os.OpenFile(reduceName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
		debug("append File %s\r\n", reduceFile)
		fileList = append(fileList, reduceFile)
	}
	debug("outFile: %s\r\n", outFile)
	of, err := os.OpenFile(outFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	if err != nil {
		debug("open file error %s\r\n", err)
		return
	}

	var keys []string
	var kMap map[string] []string
	kMap = make(map[string] []string)
	for i := range fileList {
		mapFile := fileList[i]
		//br := bufio.NewReader(mapFile)
		//lineBytes, _, err := br.ReadLine()
		allBytes, err := ioutil.ReadFile(mapFile.Name())
		if err == io.EOF {
			break
		}
		//fmt.Println(string(lineBytes))
		kvs := []KeyValue{}
		err = json.Unmarshal(allBytes, &kvs)
		debug("doReduce, kvs: %s\r\n", kvs)
		if err != nil {
			debug("json unmarshal error : %s\r\n", err)
		}

		for _, kv := range kvs{
			kMap[kv.Key] = append(kMap[kv.Key], kv.Value)
			if len(kMap[kv.Key]) == 1 {
				keys = append(keys, kv.Key)
			}
		}
	}

	for _, key := range keys {
		//err = enc.Encode(KeyValue{key, reduceF(key, vals)})
		jb, err := json.Marshal(KeyValue{key, reduceF(key, kMap[key])})
		if err != nil {
			debug("encode error: %s\r\n", err)
		}
		of.Write(jb)
		of.Write(rnb)
	}
	of.Close()
	for _, file := range fileList {
		file.Close()
	}
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// TODO 完成doReduce才能通过测试
}
