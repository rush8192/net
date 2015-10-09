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
	fmt.Printf("Heartbeat triggered\n")
	cluster.clusterLock.RLock()
	defer cluster.clusterLock.RUnlock()
	for _, member := range cluster.Members {
		if (member != cluster.Self) {
			if (member.nextIndex != cluster.LastLogEntry + 1) {
				go SendAppendRpc(&cluster.Log[member.nextIndex], member, nil)
			} else {
				go SendAppendRpc(nil, member, nil)
			}
		}
	}
	fmt.Printf("Heartbeat timer reset\n")
	cluster.electionTimer = time.AfterFunc(time.Duration(HEARTBEAT_INTERVAL)*time.Millisecond, Heartbeat)
}

func ResetHeartbeatTimer() {
	cluster.electionTimer.Stop()
	fmt.Printf("Heartbeat timer reset\n")
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
	fmt.Printf("%d entries in log; sending append rpc with index %d\n", len(cluster.Log), (member.nextIndex - 1))
	rpc.AppendRPC = AppendEntries{ 	cluster.CurrentTerm, 
									nil,
									cluster.commitIndex,
									(member.nextIndex - 1),
									cluster.Log[(member.nextIndex - 1)].Term,
									cluster.Self.Hostname  }
	if (entry != nil) {
		rpc.AppendRPC.Entries = append(make([]LogEntry, 0, 1),*entry)
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
	listenKey := GetAppendResponseListenKey(rpc, member)
	if (success != nil && needListen) {
		cluster.rpcLock.Lock()
		fmt.Printf("Setting callback channel at %s\n", listenKey)
		cluster.oustandingRPC[listenKey] = success
		cluster.rpcLock.Unlock()
	}
	encoder := gob.NewEncoder(conn)
	err = encoder.Encode(rpc)
	if (err != nil) {
		if (success != nil) {
			success <- false
		}
		cluster.rpcLock.Lock()
		delete(cluster.oustandingRPC, listenKey)
		cluster.rpcLock.Unlock()
		fmt.Printf("Encode error attempting to send AppendEntries to %s\n", member.Ip)
		
	} else {
		fmt.Printf(time.Now().String() + " Sent AppendEntries: %+v to %+v\n", rpc, member);
	}
	conn.Close()
}

func HandleAppendEntriesResponse(response AppendEntriesResponse) {
	respondKey := GetAppendResponseKey(response.Id, response.PrevLogIndex)
	fmt.Printf("Checking callback channel at %s\n", respondKey)
	channel, ok := cluster.oustandingRPC[respondKey]
	if (response.Id == "") {
		return
	}
	if (ok) {
		fmt.Printf("Found channel, sending response: %t\n",response.Success)
	}
	node := GetNodeByHostname(response.Id)
	node.nodeLock.Lock()
	defer node.nodeLock.Unlock()
	if (response.Success) {
		if (ok) {
			channel <- true
		}
		if (response.NewLogIndex + 1 > node.nextIndex) {
			node.nextIndex = response.NewLogIndex + 1
		}
		if (response.NewLogIndex > node.matchIndex) {
			node.matchIndex = response.NewLogIndex
		}
		updateCommitStatus()
	} else {
		// retry command
		if (ok) {
			channel <- false
		}
		node.nextIndex--
		if (node.nextIndex == 0) {
			node.nextIndex = 1
		}
		go SendAppendRpc(&cluster.Log[node.nextIndex], node, nil)
	}
	if (ok) {
		cluster.rpcLock.Lock()
		delete(cluster.oustandingRPC, respondKey)
		cluster.rpcLock.Unlock()
	}
}

func SetPostElectionState() {
	fmt.Printf("Won election\n");
	cluster.electionTimer.Stop() // can't timeout as leader
	cluster.Leader = cluster.Self
	cluster.Self.state = LEADER
	for _, member := range cluster.Members {
		if (member != cluster.Self) {
			member.nextIndex = cluster.LastLogEntry + 1
			member.matchIndex = 0
			member.state = MEMBER
		}
	}
	go AppendCommandToLog(&Command{})
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

