package cluster

import (
	"fmt"
	"encoding/gob"
	"net"
	"strconv"
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
									cluster.Self.Hostname,
									0  }
	if (entry != nil) {
		rpc.AppendRPC.Entries = append(make([]LogEntry, 0, 1),*entry)
		rpc.AppendRPC.CId = entry.C.CId
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

func leaderAppendToLog(command *Command) bool {
	fmt.Printf("Attempting to commit to own log and get a quorum\n")
	logEntry := &LogEntry{ *command, cluster.CurrentTerm, time.Now() }
	cluster.clusterLock.Lock()
	if (!AppendToLog(logEntry)) {
		cluster.clusterLock.Unlock()
		return false
	}
	fmt.Printf("Committed to own log\n")
	voteChannel := make(chan bool, len(cluster.Members) - 1)
	votesNeeded := (len(cluster.Members) / 2) // plus ourself to make a quorum
	for _, member := range cluster.Members {
		if (member != cluster.Self) {
			go SendAppendRpc(logEntry, member, voteChannel)
		}
	}
	ResetHeartbeatTimer()
	cluster.clusterLock.Unlock()
	fmt.Printf("Waiting for responses \n")
	return QuorumOfResponses(voteChannel, votesNeeded)
}

/*
 * Uses a channel to collect AppendEntriesResponses from members of the
 * cluster; returns true or false once outcome becomes definite, updating
 * the commitIndex as needed.
 * TODO: add timeout to return false to client
 */
func QuorumOfResponses(voteChannel chan bool, votesNeeded int) bool {
	yesVotes := 1 // ourself
	noVotes := 0
	for {
		vote := <- voteChannel
		fmt.Printf("Received vote: %t\n", vote)
		if (vote) {
			yesVotes++
			votesNeeded--
		} else {
			noVotes++
		}
		if (votesNeeded == 0) {
			fmt.Printf("Successfully committed, append success\n")
			cluster.clusterLock.Lock()
			cluster.commitIndex = cluster.LastLogEntry
			cluster.clusterLock.Unlock()
			return true
		}
		if (votesNeeded > (len(cluster.Members) - noVotes)) {
			fmt.Printf("Too many no votes, append fails\n")
			return false
		}
	}
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
	return hostname + ":" + strconv.FormatInt(logIndex, 10)
}

