package cluster

import (
	"fmt"
	"encoding/gob"
	"net"
)

/*
 * Member: respond to AppendEntries RPC
 */
func HandleAppendEntries(ae AppendEntries) {
	if (ae.LeaderId == "") {
		return
	}
	cluster.clusterLock.Lock()
	ResetElectionTimer(cluster)
	node := GetNodeByHostname(ae.LeaderId)	
	if (ae.Term > cluster.CurrentTerm) {
		cluster.CurrentTerm = ae.Term
		cluster.Self.state = MEMBER
		cluster.Leader = node
	}
	response := &Message{}
	defer SendAppendEntriesResponse(response, node)
	response.MessageType = "AppendEntriesResponse"
	aer := &response.AppendRPCResponse
	aer.Term = cluster.CurrentTerm
	aer.PrevLogIndex = ae.PrevLogIndex
	aer.Id = cluster.Self.Hostname
	aer.MemberLogIndex = cluster.Self.nextIndex - 1
	if (ae.Term < cluster.CurrentTerm ||
	     	cluster.LastLogEntry >= ae.PrevLogIndex ||
	     	cluster.Log[ae.PrevLogIndex].Term != ae.PrevLogTerm) {
	    fmt.Printf("Denied append entry request %+v\n", ae);
		aer.Success = false
		cluster.clusterLock.Unlock()
	} else {
		fmt.Printf("Accepted append entry request %+v\n", ae);
		if (int64(len(cluster.Log)) > ae.PrevLogIndex) {
			fmt.Printf("Reducing log size\n")
			cluster.Log = cluster.Log[0 : ae.PrevLogIndex]
		}
		if (ae.LeaderCommit > cluster.commitIndex) {
			cluster.commitIndex = ae.LeaderCommit
		}
		aer.Success = AppendToLog(&ae.Entries[0])
		cluster.clusterLock.Unlock()
	}
}

func SendAppendEntriesResponse(rpc *Message, target *Node) {
	conn, err := net.Dial("tcp", target.Ip + ":" + CLUSTER_PORT)
	if err != nil {
		fmt.Printf("Connection error attempting to contact %s in AppendEntriesResponse\n", target.Ip)
		return
	}
	defer conn.Close()
	encoder := gob.NewEncoder(conn)
	err = encoder.Encode(rpc)
	if (err != nil) {
		fmt.Printf("Encode error attempting to send AppendEntriesResponse to %s\n", target.Ip)
	}
}