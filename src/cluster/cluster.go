/*
 * Package cluster: uses the Raft consensus algorithm to build a simple, distributed,
 * fault-tolerant key-value state machine.
 *
 * Intended to run as a stand-alone program on each of a cluster of machines; client
 * programs communicate through named pipes with an instance of the cluster program
 * running on the same machine.
 */
package cluster

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

// Possible states for a node in the cluster
const LEADER = "LEADER" /* Leader of cluster */
const MEMBER = "MEMBER" /* Follower in cluster */
const UPDATING = "UPDATING" /* In contact with leader, logs not up-to-date*/
const UNKNOWN = "UNKNOWN" /* No contact with leader */
const REDIRECT = "REDIRECT" /* Non-participant in cluster */

// Timing constants (in ms)
const HEARTBEAT_INTERVAL = 500
const ELECTION_TIMEOUT_MIN = 4000
const ELECTION_TIMEOUT_MAX = 6000

/*
 * Represents an active cluster
 */
type Cluster struct {
	/* Basic configuration */
	Name string
	Members []*Node
	Self *Node
	Leader *Node
	
	/* used by election code */
	electionTimer *time.Timer
	votesCollected int
	
	/* persistent on all servers */
	Log []LogEntry
	VotedFor *Node
	CurrentTerm int64
	LastApplied int64
	
	/* volatile state on all servers */
	LastLogEntry int64
	commitIndex int64  // set to 0 on restart
	
	// Used to route responses to outstanding rpcs
	oustandingRPC map[string]chan bool
	rpcLock *sync.Mutex
	
	/* used to synchronize access to cluster state */
	clusterLock *sync.RWMutex
}

/*
 * Represents the current state of a single Node in the cluster
 */
type Node struct {
	/* Networking information */
	Hostname string
	Ip string
	
	/* volatile state on leader */
	nextIndex int64
	matchIndex int64
	state string
	
	/* used to synchronize access to node */
	nodeLock *sync.RWMutex
}

/* 
 * RPC messages sent from server to server
 */
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
	Entries []LogEntry
	LeaderCommit int64
	PrevLogIndex int64
	PrevLogTerm int64
	LeaderId string
}

type AppendEntriesResponse struct {
	Term int64
	PrevLogIndex int64
	NewLogIndex int64
	MemberLogIndex int64
	Id string
	Success bool
}

/* 
 * Wrapper for underlying messages
 */
type Message struct {
	MessageType string
	AppendRPC AppendEntries
	AppendRPCResponse AppendEntriesResponse
	RequestVote RequestVote
	RequestVoteResponse RequestVoteResponse
}

var C * Cluster
var cluster * Cluster

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
	node.Hostname = splitLine[0]
	node.Ip = splitLine[1]
	node.state = UNKNOWN
	return node
}

func PrintClusterInfo(cluster *Cluster) {
	fmt.Printf("Cluster: %s\n", cluster.Name)
	for index, member := range cluster.Members {
		if (member == cluster.Self) {
			fmt.Printf("Self: ")
		}
		fmt.Printf("Member %d: %s:%s\n", index, member.Hostname, member.Ip)
	}
}

/*
 * Initializes the Cluster representation for this run of the program. Membership
 * may change as nodes join the cluster or go offline
 */
func InitCluster(filename string, self * Node) * Cluster {
	rand.Seed( time.Now().UTC().UnixNano()) // seed RNG for random timeouts
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	
	cluster = new(Cluster)
	C = cluster
	
	scanner := bufio.NewScanner(file)
	scanner.Scan()
	cluster.Name = scanner.Text()
	scanner.Scan()
	numNodes, _ := strconv.Atoi(scanner.Text())
	cluster.Members = make([]*Node, numNodes)
	// scan line-by-line, nodes are of the form [hostname]\t[i]\n
	onIndex := 0
	for scanner.Scan() {
		line := scanner.Text()
		splitLine := strings.Split(line, "\t")
		if (splitLine[0] == self.Hostname) {
			cluster.Members[onIndex] = self
			cluster.Self = self
		} else {
			node := &Node{ splitLine[0], splitLine[1], 0, 0, UNKNOWN, &sync.RWMutex{} }
			cluster.Members[onIndex] = node
		}
		
		onIndex++
	}
	
	
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	cluster.Log = append(cluster.Log, LogEntry{})
	cluster.LastLogEntry = 0
	cluster.LastApplied = 0
	cluster.CurrentTerm = 0
	cluster.rpcLock = &sync.Mutex{}
	cluster.oustandingRPC = make(map[string] chan bool)
	cluster.clusterLock = &sync.RWMutex{}
	return cluster
}

/*
 * Loop that listens for incoming requests and dispatches them to 
 * the appropriate handler function
 */
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
		fmt.Printf(time.Now().String() + " Got message: %+v\n", message);
		go dispatchMessage(message)
	}
}

func dispatchMessage(message *Message) {
	switch message.MessageType {
	case "RequestVote":
		HandleVoteRequest(message.RequestVote)
	case "RequestVoteResponse":
		HandleVoteResponse(message.RequestVoteResponse)			
	case "AppendEntries":
		HandleAppendEntries(message.AppendRPC)
	case "AppendEntriesResponse":
		HandleAppendEntriesResponse(message.AppendRPCResponse)
	case "Heartbeat":
		cluster.CurrentTerm = message.AppendRPC.Term
		ResetElectionTimer(cluster)
	default:
		fmt.Printf("Unimplemented message type; resetting election timeout\n");
		result := ResetElectionTimer(cluster)
		if (result == false) {
			// failed to reset timer; now a candidate in new term
		}
	}
}

func ParseMessage(conn net.Conn) *Message {
	dec := gob.NewDecoder(conn)
	m := &Message{}
	err := dec.Decode(m)
	if (err != nil) {
		fmt.Printf("Decode error in Parse Message\n")
		//log.Fatal("denode error:", err)
	}
	return m
}

func GetNodeByHostname(hostname string) *Node {
	// TODO: set up a hash table
	for _, member := range cluster.Members {
		if (member.Hostname == hostname) {
			return member
		}
	}
	return nil
}

func GetMemberByIndex(target *Node) int {
	// TODO: set up hash table
	for i, member := range cluster.Members {
		if (member == target) {
			return i
		}
	}
	return -1
}

