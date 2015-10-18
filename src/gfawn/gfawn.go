package main

import "cluster"

import "fmt"
import "log"
import "strconv"
import "strings"

const NUM_THREADS = 20
const NUM_ITERS = 100

var client *cluster.Client

func main() {
	client = cluster.InitClient("gfawn")
	TestPutAndGet()
	client.Exit()
}

func TestPutAndGet() {
	_, err := client.Put("cluster_id", []byte("testcluster"))
	if (err != nil) {
		fmt.Printf("Failed to put cluster id: %s\n", err.Error())
		return
	}
	for iter := 0; iter < NUM_ITERS; iter++ {
		responses := make(chan string, NUM_THREADS)
		for i := 0; i < NUM_THREADS; i++ {
			num := i
			go PutAndGet(num, responses)
		}
		for i := 0; i < NUM_THREADS; i++ {
			fmt.Printf("Waiting for %dth response\n", i);
			response := <- responses
			fmt.Printf("Thread %s returning %dth\n", response, i)
			if (strings.Contains(response, "FAIL")) {
				log.Fatal("FAILED to get response")
			}
		}
	}
}

func PutAndGet(i int, responses chan string) {
	_, err := client.Put(strconv.Itoa(i), []byte(strconv.Itoa(i)))
	if (err != nil) {
		responses <- " FAIL-PUT"
		return
	}
	fmt.Printf("\tSuccessfully PUT %d\n", i)
	resultBytes, err := client.Get(strconv.Itoa(i))
	if (err != nil) {
		responses <- " FAIL-GET"
		return
	}
	result := string(resultBytes)
	fmt.Printf("\tSuccessful GET %d\n", i)
	responses <- result
	fmt.Printf("\tSent %d to channel\n", i)
}