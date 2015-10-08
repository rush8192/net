package main

import "cluster"

import "fmt"

func main() {
	cluster.InitClient("gfawn")
	cluster.Put("cluster_id", []byte("testcluster"))
	var value string = string(cluster.Get("cluster_id"))
	fmt.Printf("value for %s: %s\n", "cluster_id", value)
}
