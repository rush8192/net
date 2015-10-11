package main

import "cluster"

import "fmt"
import "strconv"

const NUM_THREADS = 10
const NUM_ITERS = 10

var client *cluster.Client

func main() {
	client = cluster.InitClient("gfawn")
	key := client.Put("cluster_id", []byte("testcluster"))
	if (key == "") {
		fmt.Printf("Failed to put cluster id\n")
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
		}
	}
	client.Exit()
}

func PutAndGet(i int, responses chan string) {
	resp := client.Put(strconv.Itoa(i), []byte(strconv.Itoa(i)))
	if (resp == "") {
		responses <- "fail"
		return
	}
	fmt.Printf("\tSuccessfully PUT %d\n", i)
	result := string(client.Get(strconv.Itoa(i)))
	fmt.Printf("\tSuccessful GET %d\n", i)
	responses <- result
	fmt.Printf("\tSent %d to channel\n", i)
}