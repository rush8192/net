package main

import "cluster"

func main() {
	self := cluster.InitSelf("cfg/self.cfg")
	clusterV := cluster.InitCluster("cfg/cluster.cfg", self)
	cluster.PrintClusterInfo(clusterV)
	cluster.ResetElectionTimer(clusterV)
	cluster.ListenForConnections(clusterV)
}