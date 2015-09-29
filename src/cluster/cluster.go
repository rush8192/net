package main

import "bufio"
import "encoding/gob"
import "fmt"
import "log"
import "net"
import "os"
import "math/rand"
import "strconv"
import "strings"
import "sync"
import "time"

const CLUSTER_PORT = "7777"
const LEADER = "LEADER"
const MEMBER = "MEMBER"
const UNKNOWN = "UNKNOWN"

const HEARTBEAT_INTERVAL = 2500
const ELECTION_TIMEOUT_MIN = 5000
const ELECTION_TIMEOUT_MAX = 10000

var cluster * Cluster

/*
 * Represents the current state of a single Node in the cluster
 */
type Node struct {
	hostname string
	ip string
	lastPing time.Time
	state string
	votedFor *Node
}

/*
 * Represents a cluster
 */
type Cluster struct {
	name string
	members []*Node
	self *Node
	leader *Node
	currentTerm int64
	electionTimer *time.Timer
	nextTimeout *time.Time
	votesCollected int
	
	clusterLock *sync.Mutex
}

type RequestVote struct {
	Term int64
	Id string
	LastTerm int64
}

type RequestVoteResponse struct {
	Term int64
	VoteGranted bool
}

type AppendEntries struct {
	Term int64
}

type Message struct {
	MessageType string
	AppendRPC AppendEntries
	RequestVote RequestVote
	RequestVoteResponse RequestVoteResponse
}

func main() {
	rand.Seed( time.Now().UTC().UnixNano()) // seed RNG for random timeouts

	self := InitSelf("cfg/self.cfg")
	cluster = InitCluster("cfg/cluster.cfg", self)
	PrintClusterInfo(cluster)
	ResetElectionTimer(cluster)
	ListenForConnections(cluster)
}

func ResetElectionTimer(cluster * Cluster) bool {
	if (cluster.electionTimer != nil) {
		result := cluster.electionTimer.Stop()
		if (result == false) {
			// failed to stop timer
			return false
		}
	}
	SetRandomElectionTimer()
	return true
}

func SetRandomElectionTimer() {
	randomTimeout := rand.Float32()*(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN) + ELECTION_TIMEOUT_MIN
	fmt.Printf("Setting random timeout: %2.2f\n", randomTimeout)
	cluster.electionTimer = time.AfterFunc(time.Duration(randomTimeout)*time.Millisecond, ElectionTimeout)
}

func ElectionTimeout() {
	cluster.clusterLock.Lock()
	fmt.Printf("Timed out, starting election in term %d\n", cluster.currentTerm)
	SetRandomElectionTimer()
	cluster.self.state = UNKNOWN
	cluster.currentTerm++
	cluster.self.votedFor = cluster.self
	cluster.votesCollected = 1
	for _, member := range cluster.members {
		if (member != cluster.self) {
			go SendVoteRequest(member)
		}
	}
	cluster.clusterLock.Unlock()
}

func PrintClusterInfo(cluster * Cluster) {
	fmt.Printf("Cluster: %s\n", cluster.name)
	for index, member := range cluster.members {
		if (member == cluster.self) {
			fmt.Printf("Self: ")
		}
		fmt.Printf("Member %d: %s:%s\n", index, member.hostname, member.ip)
	}
}

/*
 * Returns the Node representation of the local node
 */
func InitSelf(filename string) * Node {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	
	node := new(Node)
	
	scanner := bufio.NewScanner(file)
	scanner.Scan()
	line := scanner.Text()
	splitLine := strings.Split(line, "\t")
	node.hostname = splitLine[0]
	node.ip = splitLine[1]
	node.state = UNKNOWN
	return node
}

/*
 * Initializes the Cluster representation for this run of the program. Membership
 * may change as nodes join the cluster or go offline
 */
func InitCluster(filename string, self * Node) * Cluster {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	
	cluster := new(Cluster)

	
	scanner := bufio.NewScanner(file)
	scanner.Scan()
	cluster.name = scanner.Text()
	scanner.Scan()
	numNodes, _ := strconv.Atoi(scanner.Text())
	cluster.members = make([]*Node, numNodes)
	// scan line-by-line, nodes are of the form [hostname]\t[i]\n
	onIndex := 0
	for scanner.Scan() {
		line := scanner.Text()
		splitLine := strings.Split(line, "\t")
		if (splitLine[0] == self.hostname) {
			cluster.members[onIndex] = self
			cluster.self = self
		} else {
			node := &Node{ splitLine[0], splitLine[1], time.Time{}, UNKNOWN, nil }
			cluster.members[onIndex] = node
		}
		
		onIndex++
	}
	
	
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	cluster.currentTerm = 0
	cluster.clusterLock = &sync.Mutex{}
	return cluster
}

func HandleVoteRequest(vr RequestVote) {
	m := &Message{}
	m.MessageType = "RequestVoteResponse"
	cluster.clusterLock.Lock()
	if (vr.Term > cluster.currentTerm && cluster.self.votedFor == nil) {
		m.RequestVoteResponse = RequestVoteResponse{ vr.Term, true}
		ResetElectionTimer(cluster)
		cluster.currentTerm = vr.Term
		newLeader := GetNodeByHostname(vr.Id)
		cluster.leader = newLeader
		cluster.self.state = MEMBER
	} else {
		m.RequestVoteResponse = RequestVoteResponse{ cluster.currentTerm, false }
	}
	cluster.clusterLock.Unlock()
	targetNode := GetNodeByHostname(vr.Id)
	if (targetNode == nil) {
		// handle failure
		fmt.Printf("No target node found!");
		return
	}
	conn, err := net.Dial("tcp", targetNode.ip + ":" + CLUSTER_PORT)
	if err != nil {
		//log.Fatal("Connection error", err)
		fmt.Printf("Connection error attempting to contact %s in HandleVoteRequest\n", targetNode.ip)
		return
	}
	encoder := gob.NewEncoder(conn)
	err = encoder.Encode(m)
	if (err != nil) {
		fmt.Printf("Encode error attempting to respond to %s in HandleVoteRequest\n", targetNode.ip)
		//log.Fatal("encode error:", err)
	} else {
		fmt.Printf("Sent message: %+v to: %+v\n", m, targetNode);
	}
	conn.Close()
}

func GetNodeByHostname(hostname string) *Node {
	// TODO: set up a hash table
	for _, member := range cluster.members {
		if (member.hostname == hostname) {
			return member
		}
	}
	return nil
}

/*
 * Sends a heartbeat to each of the nodes in the cluster
 */ 
func Heartbeat() {
	cluster.clusterLock.Lock()
	for _, member := range cluster.members {
		if (member != cluster.self) {
			m := &Message{}
			m.MessageType = "Heartbeat"
			m.AppendRPC = AppendEntries{ cluster.currentTerm }
			conn, err := net.Dial("tcp", member.ip + ":" + CLUSTER_PORT)
			if err != nil {
				//log.Fatal("Connection error", err)
				fmt.Printf("Connection error attempting to contact %s in Heartbeat\n", member.ip)
				cluster.clusterLock.Unlock()
				return
			}
			encoder := gob.NewEncoder(conn)
			err = encoder.Encode(m)
			if (err != nil) {
				fmt.Printf("Encode error attempting to send Heartbeat to %s\n", member.ip)
				//log.Fatal("encode error:", err)
			} else {
				fmt.Printf("Sent message: %+v to %+v\n", m, member);
			}
			conn.Close()
		}
	}
	cluster.electionTimer = time.AfterFunc(time.Duration(HEARTBEAT_INTERVAL)*time.Millisecond, Heartbeat)
	cluster.clusterLock.Unlock()
}

func HandleVoteResponse(vr RequestVoteResponse) {
	if (vr.VoteGranted == true) {
		cluster.clusterLock.Lock()
		if (cluster.leader != cluster.self) {
			cluster.votesCollected++
			if (cluster.votesCollected > (len(cluster.members) / 2)) {
				cluster.electionTimer.Stop()
				cluster.leader = cluster.self
				cluster.self.state = LEADER
				go Heartbeat()
			}
		}
		cluster.clusterLock.Unlock()
	} 
}

func ListenForConnections(cluster * Cluster) {
	input, err := net.Listen("tcp", ":" + CLUSTER_PORT)
	if err != nil {
		log.Fatal(err)
	}
	for {
		fmt.Println("Listening for messages")
		conn, err := input.Accept() // this blocks until connection or error
		if err != nil {
			fmt.Printf("Error while accepting connection")
			continue
			//log.Fatal(err)
		}
		message := ParseMessage(conn)
		fmt.Printf("Got message: %+v\n", message);
		switch message.MessageType {
		case "RequestVote":
			go HandleVoteRequest(message.RequestVote)
		case "RequestVoteResponse":
			go HandleVoteResponse(message.RequestVoteResponse)			
		case "Heartbeat":
			 ResetElectionTimer(cluster)
		default:
			fmt.Printf("Unimplemented message type; resetting election timeout\n");
			result := ResetElectionTimer(cluster)
			if (result == false) {
				// failed to reset timer; now a candidate in new term
			}
		}
		
	}
}

func ParseMessage(conn net.Conn) *Message {
	dec := gob.NewDecoder(conn)
	m := &Message{}
	err := dec.Decode(m)
	if (err != nil) {
		fmt.Printf("Decode error in SendVoteRequest\n")
		//log.Fatal("encode error:", err)
	}
	return m
}

func SendVoteRequest(target *Node) {
	m := &Message{}
	m.RequestVote = RequestVote{ cluster.currentTerm, cluster.self.hostname, 0 }
	m.MessageType = "RequestVote"
	conn, err := net.Dial("tcp", target.ip + ":" + CLUSTER_PORT)
	if err != nil {
		fmt.Printf("Connection error attempting to contact %s in SendVoteRequest\n", target.ip)
		//log.Fatal("Connection error", err)
		return
	}
	encoder := gob.NewEncoder(conn)
	err = encoder.Encode(m)
	if (err != nil) {
		fmt.Printf("Encode error attempting to contact %s in SendVoteRequest\n", target.ip)
		//log.Fatal("encode error:", err)
	} else {
		fmt.Printf("Sent message: %+v to %+v\n", m, target)
	}
	conn.Close()
}

