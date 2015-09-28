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
	term int64
	id string
	lastTerm int64
}

type RequestVoteResponse struct {
	term int64
	voteGranted bool
}

type Message struct {
	messageType string
	requestVote RequestVote
	requestVoteResponse RequestVoteResponse
}

func main() {
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
	randomTimeout := rand.Float32()*(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN) + ELECTION_TIMEOUT_MIN
	fmt.Printf("Setting random timeout: %2.2f\n", randomTimeout)
	cluster.electionTimer = time.AfterFunc(time.Duration(randomTimeout)*time.Millisecond, ElectionTimeout)
	return true
}

func ElectionTimeout() {
	cluster.clusterLock.Lock()
	fmt.Printf("Timed out, starting election in term %d\n", cluster.currentTerm)
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
	var m Message
	m.messageType = "RequestVoteResponse"
	cluster.clusterLock.Lock()
	if (vr.term > cluster.currentTerm && cluster.self.votedFor == nil) {
		m.requestVoteResponse = RequestVoteResponse{ vr.term, true}
		ResetElectionTimer(cluster)
		cluster.currentTerm = vr.term
		newLeader := GetNodeByHostname(vr.id)
		cluster.leader = newLeader
		cluster.self.state = MEMBER
	} else {
		m.requestVoteResponse = RequestVoteResponse{ cluster.currentTerm, false}
	}
	cluster.clusterLock.Unlock()
	targetNode := GetNodeByHostname(vr.id)
	if (targetNode == nil) {
		// handle failure
	}
	conn, err := net.Dial("tcp", targetNode.ip + ":" + CLUSTER_PORT)
	if err != nil {
		log.Fatal("Connection error", err)
	}
	encoder := gob.NewEncoder(conn)
	encoder.Encode(m)
	conn.Close()
	
}

func GetNodeByHostname(hostname string) *Node {
	for _, member := range cluster.members {
		if (member.hostname == hostname) {
			return member
		}
	}
	return nil
}

func Heartbeat() {
	cluster.clusterLock.Lock()
	for _, member := range cluster.members {
		if (member != cluster.self) {
			var m Message
			m.messageType = "Heartbeat"
			conn, err := net.Dial("tcp", member.ip + ":" + CLUSTER_PORT)
			if err != nil {
				log.Fatal("Connection error", err)
			}
			encoder := gob.NewEncoder(conn)
			encoder.Encode(m)
			conn.Close()
		}
	}
	cluster.electionTimer = time.AfterFunc(time.Duration(HEARTBEAT_INTERVAL)*time.Millisecond, Heartbeat)
	cluster.clusterLock.Unlock()
}

func HandleVoteResponse(vr RequestVoteResponse) {
	if (vr.voteGranted == true) {
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
		conn, err := input.Accept() // this blocks until connection or error
		if err != nil {
			log.Fatal(err)
		}
		message := ParseMessage(conn)
		fmt.Println("Got message of type %s\n", message.messageType)
		switch message.messageType {
		case "RequestVote":
			go HandleVoteRequest(message.requestVote)
		case "RequestVoteResponse":
			go HandleVoteResponse(message.requestVoteResponse)
		default:
			result := ResetElectionTimer(cluster)
			if (result == false) {
				// failed to reset timer; now a candidate in new term
			}
		}
		
	}
}

func ParseMessage(conn net.Conn) Message {
	dec := gob.NewDecoder(conn)
	var m Message
	dec.Decode(m)
	return m
}

func SendVoteRequest(target *Node) {
	var m Message
	m.requestVote = RequestVote{ cluster.currentTerm, cluster.self.hostname, 0 }
	m.messageType = "RequestVote"
	conn, err := net.Dial("tcp", target.ip + ":" + CLUSTER_PORT)
	if err != nil {
		log.Fatal("Connection error", err)
	}
	encoder := gob.NewEncoder(conn)
	encoder.Encode(m)
	conn.Close()
}

