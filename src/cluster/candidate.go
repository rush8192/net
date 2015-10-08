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
	m.RequestVote = RequestVote{ cluster.CurrentTerm, cluster.Self.Hostname, 0 }
	m.MessageType = "RequestVote"
	fmt.Printf("Dialing %s\n", target.Ip)
	conn, err := net.Dial("tcp", target.Ip + ":" + CLUSTER_PORT)
	if err != nil {
		fmt.Printf("Connection error attempting to contact %s in SendVoteRequest\n", target.Ip)
		//log.Fatal("Connection error", err)
		if (retry) {
			time.Sleep((3*ELECTION_TIMEOUT_MIN/4)*time.Millisecond)
			if (cluster.Self.state == UNKNOWN) { 
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
		cluster.clusterLock.RLock()
		defer cluster.clusterLock.RUnlock()
		if (cluster.Self.state != LEADER) {
			cluster.votesCollected++
			if (cluster.votesCollected > (len(cluster.Members) / 2)) {
				SetPostElectionState()
			}
		}
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

func ResetElectionTimer(cluster * Cluster) bool {
	fmt.Printf("resetting election timer\n")
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
	cluster.clusterLock.Lock()
	defer cluster.clusterLock.Unlock()
	if (vr.Term > cluster.CurrentTerm) {
		cluster.CurrentTerm = vr.Term;
		ResetElectionTimer(cluster)
		cluster.Self.state = MEMBER
	}
	// accept vote
	if (vr.Term >= cluster.CurrentTerm && (cluster.VotedFor == nil || cluster.VotedFor == sender)) {
		m.RequestVoteResponse = RequestVoteResponse{ vr.Term, true}
		cluster.CurrentTerm = vr.Term
		cluster.VotedFor = sender
		cluster.Leader = sender
		cluster.Self.state = MEMBER
	} else {
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
	fmt.Printf("Setting random timeout: %2.2f\n", randomTimeout)
	cluster.electionTimer = time.AfterFunc(time.Duration(randomTimeout)*time.Millisecond, ElectionTimeout)
}

/*
 * Asynchronous method that fires when a follower or candidate times out
 * Resets state to begin a new term
 */
func ElectionTimeout() {
	cluster.clusterLock.Lock()
	defer cluster.clusterLock.Unlock()
	cluster.Self.state = UNKNOWN
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