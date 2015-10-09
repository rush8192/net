package main

import "cluster"

import "fmt"
import "strconv"

func main() {
	cluster.InitClient("gfawn")
	cluster.Put("cluster_id", []byte("testcluster"))
	cluster.Put("dollars", []byte("64"))
	cluster.Delete("dollars")
	var value string = string(cluster.Get("cluster_id"))
	fmt.Printf("value for %s: %s\n", "cluster_id", value)
	dollars, _ := strconv.Atoi(string(cluster.Get("dollars")))
	fmt.Printf("Dollars: %d\n", dollars)
}
