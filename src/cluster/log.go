package cluster

import "encoding/gob"
import "fmt"
import "log"
import "math"
import "os"
import "time"

const TMP_LOG_NAME = ".cluster/.tmplog"
const LOG_NAME = ".cluster/log"

type LogEntry struct {
	C Command
	Term int64
	Timestamp time.Time
}

/*
 * Handles a client command by appending it to the log or by
 * retrieving the value for a GET request key
 */
func AppendCommandToLog(command *Command) {	
	if (command.CType == GET) {
		if (VERBOSE > 0) {
			fmt.Printf("Handling get request: %+v\n", command)
		}
		handleGet(command)
	} else if (command.CType == COMMIT) {
		// not implemented
	} else {
		cluster.clusterLock.RLock()
		if (VERBOSE > 0) {
			fmt.Printf("Attempting to append command %+v to log\n", command)
		}
		if (cluster.Self == cluster.Leader) {
			cluster.clusterLock.RUnlock()
		    if (!leaderAppendToLog(command)) {
		    	command.CType = FAILED
		    }
		} else {
			cluster.clusterLock.RUnlock()
			followerAppendToLog(command)
		}
	}
	
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
			maxToSend := int64(math.Min(float64(len(cluster.Log)), float64(member.nextIndex + MAX_LOG_ENTRIES_PER_RPC)))
			if (maxToSend - member.nextIndex != 0) {
				go SendAppendRpc(cluster.Log[member.nextIndex:maxToSend], member, voteChannel, commandLogIndex)
			} else {
				fmt.Printf("## empty rpc; alraedy sent by heartbeat?\n")
			}
		}
	}
	ResetHeartbeatTimer()
	if (VERBOSE > 1) {
		fmt.Printf("Waiting for responses \n")
	}
	return LeaderQuorumOfResponses(voteChannel, votesNeeded, logIndex)
}

///////////////////////////////////////////////////////////
//
// Functions for persisting/loading log and cluster state to disk
//
func SaveStateToFile()  error {
	tmpLogFile, err := os.Create(TMP_LOG_NAME)
	if (err != nil) {
		return err
	}
	defer tmpLogFile.Close()
	encoder := gob.NewEncoder(tmpLogFile)
	err = encoder.Encode(cluster)
	if (err != nil) {
		return err
	}
	renameErr := os.Rename(TMP_LOG_NAME, LOG_NAME)
	if (renameErr != nil) {
		return renameErr // TODO: try to recover?
	}
	if (VERBOSE > 0) {
		fmt.Printf("Saved cluster state to file %s\n", LOG_NAME)
	}
	return nil
}

func LoadStateFromFile() (*Cluster, error) {
	cl, err := loadStateFromFile(LOG_NAME)
	if (err != nil) {
		fmt.Println(err)
		cl, err = loadStateFromFile(TMP_LOG_NAME)
		if (err != nil) {
			fmt.Println(err) 
		}
	}
	return cl, err
}

func loadStateFromFile(filename string) (*Cluster, error) {
	logFile, err := os.Open(filename)
	if (err != nil) {
		return nil, err
	}
	clusterFromFile := &Cluster{}
	decoder := gob.NewDecoder(logFile)
	err = decoder.Decode(clusterFromFile)
	if (err != nil) {
		return nil, err
	}
	for _, member := range clusterFromFile.Members {
		if (member.Hostname == clusterFromFile.Self.Hostname) {
			clusterFromFile.Self = member
		}
	}
	return clusterFromFile, nil
}

func AppendToLog(entry []LogEntry) bool {
	cluster.Log = append(cluster.Log, entry...)
	cluster.LastLogEntry += int64(len(entry))
	if (cluster.LastLogEntry >= int64(len(cluster.Log))) {
		log.Fatal("LastLogEntry too high\n")
	}
	return CommitLog()
}

func followerAppendToLog(command *Command) {

}

func CommitLog() bool {
	writeErr := SaveStateToFile()
	if (writeErr != nil) {
		fmt.Println(writeErr)
		return false
	}
	return true
}