package cluster

import (
	"fmt"
	"encoding/gob"
	"net"
)

/*
 * Member: respond to AppendEntries RPC
 */
func HandleAppendEntries(ae *AppendEntries) {
	cluster.clusterLock.Lock()
	node := GetNodeByHostname(ae.LeaderId)	
	if (ae.Term > cluster.CurrentTerm) {
		cluster.CurrentTerm = ae.Term
		cluster.Self.State = MEMBER
		cluster.Leader = node
		cluster.VotedFor = nil
	}
	if (ae.LeaderId == "" || cluster.Self.State != MEMBER) {
		cluster.clusterLock.Unlock()
		return
	}
	ResetElectionTimer(cluster)
	response := &Message{}
	response.AppendRPCResponse = &AppendEntriesResponse{}
	response.MessageType = "AppendEntriesResponse"
	aer := response.AppendRPCResponse
	if (ae.Term < cluster.CurrentTerm ||
	     	cluster.LastLogEntry < ae.PrevLogIndex ||
	     	cluster.Log[ae.PrevLogIndex].Term != ae.PrevLogTerm) {
	    if (VERBOSE > 0) {
	    	fmt.Printf("Denied append entry request %+v\n", ae);
	    	fmt.Printf("Message term %d differs vs our term %d\n", ae.Term, cluster.CurrentTerm)
	    	fmt.Printf("Last log entry %d differs vs Prev from msg %d\n", cluster.LastLogEntry, ae.PrevLogIndex )
	    }
	    if (cluster.LastLogEntry >= ae.PrevLogIndex) {
	    	if (VERBOSE > 0) {
	    		fmt.Printf("log entry terms differ: %d vs %d from msg\n", cluster.Log[ae.PrevLogIndex].Term, ae.PrevLogTerm)
	    	}
	    }
		aer.Success = false
		cluster.clusterLock.Unlock()
	} else {
		fmt.Printf("Accepted append entry request for log index %d\n", ae.PrevLogIndex + 1);
		numNewEntries := int64(len(ae.Entries))
		if (int64(len(cluster.Log)) - numNewEntries > ae.PrevLogIndex) {
			if (VERBOSE > 1) {
				fmt.Printf("Reducing log size from %d to %d\n", len(cluster.Log), ae.PrevLogIndex + 1)
			}
			cluster.Log = cluster.Log[0 : (ae.PrevLogIndex + 1)]
			cluster.LastLogEntry = ae.PrevLogIndex
		}
		if (ae.LeaderCommit > cluster.commitIndex) {
			cluster.commitIndex = ae.LeaderCommit
		}
		aer.Success = AppendToLog(ae.Entries)
		if (aer.Success) {
			aer.NewLogIndex = ae.PrevLogIndex + numNewEntries
		}
		cluster.clusterLock.Unlock()
	}
	aer.Term = cluster.CurrentTerm
	aer.PrevLogIndex = ae.PrevLogIndex
	aer.Id = cluster.Self.Hostname
	aer.MemberLogIndex = cluster.LastLogEntry
	aer.CId = ae.CId
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