package cluster

import (
	"fmt"
	"encoding/gob"
	"math/rand"
	"net"
	"time"
)

/*
 * Candidate: send vote request
 */
func SendVoteRequest(target *Node, retry bool) {
	m := &Message{}
	fmt.Printf("Cluster log has length %d last entry is %d\n", len(cluster.Log), cluster.LastLogEntry)
	m.RequestVote = RequestVote{ cluster.CurrentTerm, cluster.Self.Hostname, cluster.LastLogEntry, 
								 cluster.Log[cluster.LastLogEntry].Term }
	m.MessageType = "RequestVote"
	fmt.Printf("Dialing %s\n", target.Ip)
	conn, err := net.Dial("tcp", target.Ip + ":" + CLUSTER_PORT)
	if err != nil {
		fmt.Printf("Connection error attempting to contact %s in SendVoteRequest\n", target.Ip)
		//log.Fatal("Connection error", err)
		if (retry) {
			time.Sleep((3*ELECTION_TIMEOUT_MIN/4)*time.Millisecond)
			if (cluster.Self.State == UNKNOWN) { 
				SendVoteRequest(target, false)
			}
		}
		return
	}
	defer conn.Close()
	fmt.Printf("Encoding message to %s\n", target.Ip)
	encoder := gob.NewEncoder(conn)
	err = encoder.Encode(m)
	if (err != nil) {
		fmt.Printf("Encode error attempting to contact %s in SendVoteRequest\n", target.Ip)
		//log.Fatal("encode error:", err)
	} else {
		fmt.Printf(time.Now().String() + " Sent message: %+v to %+v\n", m, target)
	}
}

/*
 * Candidate: handle vote request response
 */
func HandleVoteResponse(vr RequestVoteResponse) {
	if (vr.VoteGranted == true) {
		cluster.clusterLock.Lock()
		defer cluster.clusterLock.Unlock()
		if (cluster.Self.State != LEADER) {
			cluster.votesCollected++
			if (cluster.votesCollected > (len(cluster.Members) / 2)) {
				SetPostElectionState()
			}
		}
	} 
}

func ResetElectionTimer(cluster * Cluster) bool {	
	if (cluster.electionTimer != nil) {
		result := cluster.electionTimer.Stop()
		if (result == false) {
			// failed to stop timer
			return false
		}
	}
	cluster.VotedFor = nil
	SetRandomElectionTimer()
	return true
}

func HandleVoteRequest(vr RequestVote) {
	m := &Message{}
	m.MessageType = "RequestVoteResponse"
	sender := GetNodeByHostname(vr.Id)
	fmt.Printf("Got vote request from %s\n", vr.Id)
	cluster.clusterLock.Lock()
	defer cluster.clusterLock.Unlock()
	if (vr.Term > cluster.CurrentTerm) {
		cluster.CurrentTerm = vr.Term;
		ResetElectionTimer(cluster)
		cluster.Self.State = MEMBER
		cluster.VotedFor = nil
	}
	// accept vote
	fmt.Printf("Cluster log has length %d last entry is %d\n", len(cluster.Log), cluster.LastLogEntry)
	if (vr.Term >= cluster.CurrentTerm && (cluster.VotedFor == nil || cluster.VotedFor == sender) &&
			cluster.Log[cluster.LastLogEntry].Term <= vr.LastLogTerm &&
			cluster.LastLogEntry <=  vr.LastLogIndex) {
		m.RequestVoteResponse = RequestVoteResponse{ vr.Term, true}
		cluster.CurrentTerm = vr.Term
		cluster.VotedFor = sender
		cluster.Leader = sender
		fmt.Printf("Accepting vote from %s\n", vr.Id)
		cluster.Self.State = MEMBER
	} else {
		fmt.Printf("Rejecting vote request from %s\n", vr.Id)
		m.RequestVoteResponse = RequestVoteResponse{ cluster.CurrentTerm, false }
	}
	targetNode := GetNodeByHostname(vr.Id)
	if (targetNode == nil) {
		// handle failure
		fmt.Printf("No target node found!");
		return
	}
	go SendVoteRequestResponse(m, targetNode);
}

func SendVoteRequestResponse(m *Message, target *Node) {
	conn, err := net.Dial("tcp", target.Ip + ":" + CLUSTER_PORT)
	if err != nil {
		//log.Fatal("Connection error", err)
		fmt.Printf("Connection error attempting to contact %s in HandleVoteRequest\n", target.Ip)
		return
	}
	encoder := gob.NewEncoder(conn)
	err = encoder.Encode(m)
	if (err != nil) {
		fmt.Printf("Encode error attempting to respond to %s in HandleVoteRequest\n", target.Ip)
		//log.Fatal("encode error:", err)
	} else {
		fmt.Printf(time.Now().String() + " Sent message: %+v to: %+v\n", m, target);
	}
	conn.Close()
}

func SetRandomElectionTimer() {
	randomTimeout := rand.Float32()*(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN) + ELECTION_TIMEOUT_MIN
	if (VERBOSE > 1) {
		fmt.Printf("Setting random timeout: %2.2f\n", randomTimeout)
	}
	cluster.electionTimer = time.AfterFunc(time.Duration(randomTimeout)*time.Millisecond, ElectionTimeout)
}

/*
 * Asynchronous method that fires when a follower or candidate times out
 * Resets state to begin a new term
 */
func ElectionTimeout() {
	cluster.clusterLock.Lock()
	defer cluster.clusterLock.Unlock()
	cluster.Self.State = UNKNOWN
	cluster.CurrentTerm++
	fmt.Printf("Timed out, starting election in term %d\n", cluster.CurrentTerm)
	cluster.VotedFor = cluster.Self
	cluster.votesCollected = 1
	for _, member := range cluster.Members {
		if (member != cluster.Self) {
			go SendVoteRequest(member, true)
		}
	}
	SetRandomElectionTimer()
}