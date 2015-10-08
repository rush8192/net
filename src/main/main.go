package main

import "cluster"

func main() {
	self := cluster.InitSelf("cfg/self.cfg")
	clusterV := cluster.InitCluster("cfg/cluster.cfg", self)
	cluster.ResetElectionTimer(clusterV)
	go cluster.ListenForConnections(clusterV)
	cluster.ListenForClients(cluster.REGISTER_PIPE);
}