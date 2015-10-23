package cluster

import "fmt"

/*
 * Handles a GET command from the state machine, updating the state machine
 * from conflicting log entries if needed.
 */
func handleGet(command *Command) {
	if (cluster.Self.State == LEADER || cluster.Self.State == MEMBER) {
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