package cluster

import (
	"fmt"
	"encoding/gob"
	"net"
	"time"
)

 /*
 * Leader: Sends a heartbeat (empty AppendEntries RPC) to each of the nodes in the cluster
 */ 
func Heartbeat() {
	cluster.clusterLock.RLock()
	defer cluster.clusterLock.RUnlock()
	for _, member := range cluster.Members {
		if (member != cluster.Self) {
			go SendAppendRpc(nil, member, nil)
		}
	}
	cluster.electionTimer = time.AfterFunc(time.Duration(HEARTBEAT_INTERVAL)*time.Millisecond, Heartbeat)
}

func ResetHeartbeatTimer() {
	cluster.electionTimer.Stop()
	cluster.electionTimer = time.AfterFunc(time.Duration(HEARTBEAT_INTERVAL)*time.Millisecond, Heartbeat)
}

/*
 * Leader: Send AppendEntries Rpc
 */
func SendAppendRpc(entry *LogEntry, member *Node, success chan bool) {
	var needListen bool = true
	rpc := &Message{}
	if (entry != nil) {
		rpc.MessageType = "AppendEntries"
	} else {
		rpc.MessageType = "Heartbeat"
		needListen = false
	}
	rpc.AppendRPC = AppendEntries{ 	cluster.CurrentTerm, 
									nil,
									cluster.commitIndex,
									(member.nextIndex - 1),
									cluster.Log[(member.nextIndex - 1)].Term,
									cluster.Self.Hostname  }
	if (entry != nil) {
		rpc.AppendRPC.Entries = append(make([]LogEntry, 1),*entry)
	}
	conn, err := net.Dial("tcp", member.Ip + ":" + CLUSTER_PORT)
	if err != nil {
		if (success != nil) {
			success <- false
			needListen = false
		}
		fmt.Printf("Connection error attempting to contact %s while sending AE RPC\n", member.Ip)
		return
	}
	encoder := gob.NewEncoder(conn)
	err = encoder.Encode(rpc)
	if (err != nil) {
		if (success != nil) {
			success <- false
			needListen = false
		}
		fmt.Printf("Encode error attempting to send AppendEntries to %s\n", member.Ip)
		
	} else {
		fmt.Printf(time.Now().String() + " Sent AppendEntries: %+v to %+v\n", rpc, member);
	}
	conn.Close()
	if (success != nil && needListen) {
		cluster.rpcLock.Lock()
		listenKey := GetAppendResponseListenKey(rpc, member)
		fmt.Printf("Setting callback channel at %s\n", listenKey)
		cluster.oustandingRPC[listenKey] = success
		cluster.rpcLock.Unlock()
	}
}

func HandleAppendEntriesResponse(response AppendEntriesResponse) {
	respondKey := GetAppendResponseKey(response.Id, response.PrevLogIndex)
	fmt.Printf("Checking callback channel at %s\n", respondKey)
	channel, ok := cluster.oustandingRPC[respondKey]
	if (!ok || response.Id == "") {
		//drop message; no client waiting on outcome
		return
	}
	fmt.Printf("Found channel, sending response: %t\n",response.Success)
	node := GetNodeByHostname(response.Id)
	node.nodeLock.Lock()
	defer node.nodeLock.Unlock()
	if (response.Success) {
		channel <- true
		if (response.NewLogIndex > node.nextIndex) {
			node.nextIndex = response.NewLogIndex
		}
		if (response.NewLogIndex > node.matchIndex) {
			node.matchIndex = response.NewLogIndex
		}
		updateCommitStatus()
	} else {
		// retry command
		channel <- false
		node.nextIndex--
		if (node.nextIndex == 0) {
			node.nextIndex = 1
		}
		go SendAppendRpc(&cluster.Log[response.PrevLogIndex + 1], node, nil)
		
	}
	cluster.rpcLock.Lock()
	delete(cluster.oustandingRPC, respondKey)
	cluster.rpcLock.Unlock()
}

func updateCommitStatus() {
	// TODO: update commit index
}

/* combine node id with index of appendEntries rpc 
 * to create unique key to route response */
func GetAppendResponseListenKey(rpc *Message, member *Node) string {
	return GetAppendResponseKey(member.Hostname, rpc.AppendRPC.PrevLogIndex)
}

func GetAppendResponseKey (hostname string, logIndex int64) string {
	return hostname + ":" + string(logIndex)
}

