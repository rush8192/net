package cluster

import "fmt"
import "log"
import "time"

type LogEntry struct {
	C Command
	Term int64
	Timestamp time.Time
}

func AppendCommandToLog(command *Command) {
	cluster.clusterLock.RLock()
	fmt.Printf("Got cluster lock\n")
	if (command.CType == GET) {
		fmt.Printf("Handling get request: %+v\n", command)
		handleGet(command)
		cluster.clusterLock.RUnlock()
	} else if (command.CType == COMMIT) {
		// not implemented
	} else {
		fmt.Printf("Attempting to append command %+v to log\n", command)
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

func handleGet(command *Command) {
	if (cluster.Self.state == LEADER || cluster.Self.state == MEMBER) {
		highestConflictingEntry := int64(-1)
		fmt.Printf("Finding highest conflicting entry\n")
		for i, entry := range cluster.Log {
			if (int64(i) <= cluster.LastApplied) {
				continue
			}
			if (entry.C.Key == command.Key) {
				highestConflictingEntry = int64(i)
				fmt.Printf("Found entry in log with key from GET command\n")
			}
		}
		if (highestConflictingEntry == int64(-1) || 
				updateStateMachineToLogIndex(highestConflictingEntry)) {
			fmt.Printf("Fetching value from backing store\n")
			command.Value = StoreGet(command.Key)
		}
	}
}

/* Updates the state machine up to the min of (commitIndex, logIndex) 
 * Must hold cluster lock when calling this method */
func updateStateMachineToLogIndex(logIndex int64) bool {
	var appliedEntries int64
	// keep first no-op entry in log, apply others
	for appliedEntries = cluster.LastApplied + 1; 
			appliedEntries <= logIndex && appliedEntries <= cluster.commitIndex; 
			appliedEntries++ {
		fmt.Println(cluster.Log)
		fmt.Printf("Applying command at index %d\n", appliedEntries)
		success := ApplyToStateMachine(cluster.Log[appliedEntries])
		if (!success) {
			fmt.Printf("Failed to apply entries to state machine\n")
			break
		}
	}
	appliedEntries--
	fmt.Printf("Successfully applied %d log entries to state machine\n", appliedEntries)
	return true
}

func ApplyToStateMachine(entry LogEntry) bool {
	var success bool
	switch entry.C.CType {
	case NOOP: // do nothing
		success = true
	case PUT:
		fallthrough
	case UPDATE:
		fmt.Printf("Placing value in file for key %s\n", entry.C.Key) 
		success = StorePut(entry.C.Key, entry.C.Value)
	case DELETE:
		fmt.Printf("Deleting file for key %s\n", entry.C.Key) 
		success = StoreDelete(entry.C.Key)
	default:
		fmt.Printf("Invalid command type in log %d", entry.C.CType)
	}
	if (success) {
		cluster.LastApplied++
		success = SaveStateToFile()
	} else {
		fmt.Printf("Failed to apply command for entry %+v\n", entry)
	}
	return success
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
	votes := make(chan bool, len(cluster.Members) - 1)
	votesNeeded := (len(cluster.Members) / 2) // plus ourself to make a quorum
	for _, member := range cluster.Members {
		if (member != cluster.Self) {
			go SendAppendRpc(logEntry, member, votes)
		}
	}
	ResetHeartbeatTimer()
	cluster.clusterLock.Unlock()
	fmt.Printf("Waiting for responses \n")
	yesVotes := 1 // ourself
	noVotes := 0
	for {
		vote := <- votes
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

func SaveStateToFile() bool {
	return true
}

func AppendToLog(entry *LogEntry) bool {
	cluster.Log = append(cluster.Log, *entry)
	cluster.LastLogEntry++
	if (cluster.LastLogEntry >= int64(len(cluster.Log))) {
		log.Fatal("LastLogEntry too high\n")
	}
	return CommitLog()
}

func followerAppendToLog(command *Command) {

}

func CommitLog() bool {
	return true
}