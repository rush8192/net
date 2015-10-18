package cluster

import "encoding/gob"
import "fmt"
import "log"
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

func handleGet(command *Command) {
	if (cluster.Self.state == LEADER || cluster.Self.state == MEMBER) {
		highestConflictingEntry := int64(-1)
		if (VERBOSE > 1) {
			fmt.Printf("Finding highest conflicting entry\n")
		}
		for i, entry := range cluster.Log {
			if (int64(i) <= cluster.LastApplied) {
				continue
			}
			if (entry.C.Key == command.Key) {
				highestConflictingEntry = int64(i)
				if (VERBOSE > 1) {
					fmt.Printf("Found entry in log with key from GET command\n")
				}
			}
		}
		cluster.clusterLock.Lock()
		if (highestConflictingEntry == int64(-1) || 
				updateStateMachineToLogIndex(highestConflictingEntry)) {
			if (VERBOSE > 1) {
				fmt.Printf("Fetching value from backing store\n")
			}
			command.Value = StoreGet(command.Key)
		}
		cluster.clusterLock.Unlock()
	}
}

/* Updates the state machine up to the min of (commitIndex, logIndex) 
 * Must hold cluster lock when calling this method */
func updateStateMachineToLogIndex(logIndex int64) bool {
	var appliedEntries int64
	// keep first no-op entry in log, apply others
	fmt.Printf("Commit index at %d\n", cluster.commitIndex)
	for appliedEntries = cluster.LastApplied + 1; 
			appliedEntries <= logIndex && appliedEntries <= cluster.commitIndex; 
			appliedEntries++ {
		fmt.Printf("Applying command at index %d\n", appliedEntries)
		success := ApplyToStateMachine(cluster.Log[appliedEntries])
		if (!success) {
			fmt.Printf("### Failed to apply entries to state machine #####\n")
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
		writeErr := SaveStateToFile()
		if (writeErr != nil) {
			fmt.Println(writeErr)
			return false
		}
		success = true
	} else {
		fmt.Printf("Failed to apply command for entry %+v\n", entry)
	}
	return success
}

func SaveStateToFile()  error {
	tmpLogFile, err := os.Create(TMP_LOG_NAME)
	if (err != nil) {
		return err
	}
	encoder := gob.NewEncoder(tmpLogFile)
	err = encoder.Encode(cluster)
	if (err != nil) {
		return err
	}
	renameErr := os.Rename(TMP_LOG_NAME, LOG_NAME)
	if (renameErr != nil) {
		return renameErr // TODO: try to recover?
	}
	return nil
}

func LoadStateFromFile() (*Cluster, error) {
	cl, err := loadStateFromFile(LOG_NAME)
	if (err != nil) {
		cl, err = loadStateFromFile(TMP_LOG_NAME)
	}
	return cl, err
}

func loadStateFromFile(filename string) (*Cluster, error) {
	tmpLogFile, err := os.Create(filename)
	if (err != nil) {
		return nil, err
	}
	clusterFromFile := &Cluster{}
	decoder := gob.NewDecoder(tmpLogFile)
	err = decoder.Decode(clusterFromFile)
	if (err != nil) {
		return nil, err
	}
	return clusterFromFile, nil
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
	writeErr := SaveStateToFile()
	if (writeErr != nil) {
		fmt.Println(writeErr)
		return false
	}
	return true
}