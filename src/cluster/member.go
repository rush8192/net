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
		cluster.VotedFor = nil
	}
	response := &Message{}
	response.MessageType = "AppendEntriesResponse"
	aer := &response.AppendRPCResponse
	aer.Term = cluster.CurrentTerm
	aer.PrevLogIndex = ae.PrevLogIndex
	aer.Id = cluster.Self.Hostname
	aer.MemberLogIndex = cluster.LastLogEntry
	aer.CId = ae.CId
	if (ae.Term < cluster.CurrentTerm ||
	     	cluster.LastLogEntry < ae.PrevLogIndex ||
	     	cluster.Log[ae.PrevLogIndex].Term != ae.PrevLogTerm) {
	    fmt.Println(cluster.Log)
	    fmt.Printf("Denied append entry request %+v\n", ae);
	    fmt.Printf("Message term %d differs vs our term %d\n", ae.Term, cluster.CurrentTerm)
	    fmt.Printf("Last log entry %d differs vs Prev from msg %d\n", cluster.LastLogEntry, ae.PrevLogIndex )
	    if (cluster.LastLogEntry >= ae.PrevLogIndex) {
	    	fmt.Printf("log entry terms differ: %d vs %d from msg\n", cluster.Log[ae.PrevLogIndex].Term, ae.PrevLogTerm)
	    }
		aer.Success = false
		cluster.clusterLock.Unlock()
	} else {
		fmt.Printf("Accepted append entry request %+v\n", ae);
		if (int64(len(cluster.Log) - 1) > ae.PrevLogIndex) {
			fmt.Println(cluster.Log)
			fmt.Printf("Reducing log size from %d to %d\n", len(cluster.Log), ae.PrevLogIndex + 1)
			cluster.Log = cluster.Log[0 : (ae.PrevLogIndex + 1)]
			cluster.LastLogEntry = ae.PrevLogIndex
		}
		if (ae.LeaderCommit > cluster.commitIndex) {
			cluster.commitIndex = ae.LeaderCommit
		}
		aer.Success = AppendToLog(&ae.Entries[0])
		if (aer.Success) {
			aer.NewLogIndex = ae.PrevLogIndex + 1
		}
		cluster.clusterLock.Unlock()
	}
	SendAppendEntriesResponse(response, node)
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