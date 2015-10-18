package main

import "cluster"
import "fmt"

func main() {
	clusterV, err := cluster.InitCluster("cfg/cluster.cfg", "cfg/self.cfg")
	if (err != nil) {
		fmt.Println(err)
		return
	}
	cluster.ResetElectionTimer(clusterV)
	go cluster.ListenForConnections(clusterV)
	cluster.ListenForClients(cluster.REGISTER_PIPE);
}