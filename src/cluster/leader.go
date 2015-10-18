package cluster

import (
	"fmt"
	"encoding/gob"
	"math"
	"net"
	"strconv"
	"time"
)

const MAX_LOG_ENTRIES_PER_RPC = 50

 /*
 * Leader: Sends a heartbeat (empty AppendEntries RPC) to each of the nodes in the cluster
 */ 
func Heartbeat() {
	if (VERBOSE > 1) {
		fmt.Printf("Heartbeat triggered\n")
	}
	for _, member := range cluster.Members {
		if (member != cluster.Self) {
			if (member.nextIndex != cluster.LastLogEntry + 1) {
				maxToSend := int(math.Min(float64(len(cluster.Log)), float64(member.nextIndex + MAX_LOG_ENTRIES_PER_RPC)))
				go SendAppendRpc(cluster.Log[member.nextIndex : maxToSend], member, nil, 0)
			} else {
				go SendAppendRpc(nil, member, nil, 0)
			}
		}
	}
	if (VERBOSE > 1) {
		fmt.Printf("Heartbeat timer reset\n")
	}
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
func SendAppendRpc(entry []LogEntry, member *Node, success chan bool, logIndex int64) {
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
		rpc.AppendRPC.Entries = append(make([]LogEntry, 0, 1), entry...)
		rpc.AppendRPC.CId = entry[len(entry) - 1].C.CId
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
	listenKey := GetAppendResponseKey(member.Hostname, rpc.AppendRPC.CId, logIndex )
	if (success != nil && needListen) {
		cluster.rpcLock.Lock()
		fmt.Printf("Setting callback channel at %s\n", listenKey)
		cluster.oustandingRPC[listenKey] = success
		cluster.rpcLock.Unlock()
	}
	encoder := gob.NewEncoder(conn)
	err = encoder.Encode(rpc)
	if (err != nil) {
		go SendAppendRpc(entry, member, nil, 0) // retry
		fmt.Printf("Encode error attempting to send AppendEntries to %s\n", member.Ip)
	} else {
		if (VERBOSE > 1) {
			fmt.Printf(time.Now().String() + " Sent AppendEntries: %+v to %+v\n", rpc, member);
		}
	}
	conn.Close()
}

func HandleAppendEntriesResponse(response AppendEntriesResponse) {
	respondKey := GetAppendResponseKey(response.Id, response.CId, response.MemberLogIndex - 1)
	channel, ok := cluster.oustandingRPC[respondKey]
	if (response.Id == "") {
		return
	}
	if (ok) {
		if (VERBOSE > 0) {
			fmt.Printf("Found channel at %s, sending response: %t\n", respondKey, response.Success)
		}
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
		
		node.nextIndex = int64(math.Min(float64(response.MemberLogIndex + 1), float64(response.PrevLogIndex)))
		maxToSend := int(math.Min(float64(len(cluster.Log)), float64(node.nextIndex + MAX_LOG_ENTRIES_PER_RPC)))
		go SendAppendRpc(cluster.Log[node.nextIndex : 
			maxToSend], node, nil, 0)
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
	logEntry := &LogEntry{ *command, cluster.CurrentTerm, time.Now() }
	cluster.clusterLock.Lock()
	commandLogIndex := cluster.LastLogEntry
	if (!AppendToLog(append(make([]LogEntry, 0, 1), *logEntry))) {
		cluster.clusterLock.Unlock()
		return false
	}
	logIndex := cluster.LastLogEntry
	if (VERBOSE > 1) {
		fmt.Printf("Committed to own log\n")
	}
	voteChannel := make(chan bool, len(cluster.Members) - 1)
	votesNeeded := (len(cluster.Members) / 2) // plus ourself to make a quorum
	for _, member := range cluster.Members {
		if (member != cluster.Self) {
			maxToSend := int(math.Min(float64(len(cluster.Log)), float64(member.nextIndex + MAX_LOG_ENTRIES_PER_RPC)))
			go SendAppendRpc(cluster.Log[member.nextIndex:maxToSend], member, voteChannel, commandLogIndex)
		}
	}
	ResetHeartbeatTimer()
	if (VERBOSE > 1) {
		fmt.Printf("Waiting for responses \n")
	}
	return QuorumOfResponses(voteChannel, votesNeeded, logIndex)
}

/*
 * Uses a channel to collect AppendEntriesResponses from members of the
 * cluster; returns true or false once outcome becomes definite, updating
 * the commitIndex as needed.
 */
func QuorumOfResponses(voteChannel chan bool, votesNeeded int, logIndex int64) bool {
	defer cluster.clusterLock.Unlock()
	yesVotes := 1 // ourself
	noVotes := 0
	timeout := time.After(TIMEOUT_MS * time.Millisecond)
	for {
		var vote bool
		select {
		case <- timeout:
			fmt.Printf("Timeout on request\n")
			return false
		case vote = <- voteChannel:
		}
		if (VERBOSE > 1) {
			fmt.Printf("Received vote: %t\n", vote)
		}
		if (vote) {
			yesVotes++
			votesNeeded--
		} else {
			noVotes++
		}
		if (votesNeeded == 0) {
			if (VERBOSE > 0) {
				fmt.Printf("Successfully committed %d, append success\n", logIndex)
			}
			cluster.commitIndex = logIndex
			return true
		}
		if (votesNeeded > (len(cluster.Members) - (noVotes + yesVotes))) {
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
	return GetAppendResponseKey(member.Hostname, rpc.AppendRPC.CId, rpc.AppendRPC.PrevLogIndex )
}

func GetAppendResponseKey (hostname string, cid int64, logIndex int64) string {
	return hostname + ":" + strconv.FormatInt(cid, 10) + ":" + strconv.FormatInt(logIndex, 10)
}

