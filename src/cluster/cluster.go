package main

import "bufio"
import "fmt"
import "log"
import "os"
import "strconv"
import "strings"
import "time"

/*
 * Represents the current state of a single Node in the cluster
 */
type Node struct {
	hostname string
	ip string
	lastPing time.Time
}

/*
 * Represents a cluster
 */
type Cluster struct {
	name string
	members []*Node
	self *Node
	leader *Node
}

func main() {
	self := InitSelf("cfg/self.cfg")
	cluster := InitCluster("cfg/cluster.cfg", self)
	PrintClusterInfo(cluster, self)
}

func PrintClusterInfo(cluster * Cluster, self * Node) {
	fmt.Printf("Cluster: %s\n", cluster.name)
	for index, member := range cluster.members {
		if (member == self) {
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
			node := &Node{ splitLine[0], splitLine[1], time.Time{} }
			cluster.members[onIndex] = node
		}
		
		onIndex++
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return cluster
}