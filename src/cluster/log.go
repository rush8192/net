package cluster

import "fmt"
import "time"

type LogEntry struct {
	C *Command
	Term int64
	Timestamp time.Time
}

func AppendCommandToLog(command *Command) {
	cluster.clusterLock.Lock()
	defer cluster.clusterLock.Unlock()
	if (command.CType == GET) {
		handleGet(command)
	} else if (command.CType == COMMIT) {
	
	} else if (command.CType == NOOP) {
	
	} else {
		if (cluster.Self == cluster.Leader) {
		    if (!leaderAppendToLog(command)) {
		    	command.CType = FAILED
		    }
		} else {
			followerAppendToLog(command)
		}
	}
	
}

func handleGet(command *Command) {
	if (cluster.Self.state == LEADER || cluster.Self.state == MEMBER) {
		command.Value = StoreGet(command.Key)
	}
}

func leaderAppendToLog(command *Command) bool {
	logEntry := &LogEntry{ command, cluster.CurrentTerm, time.Now() }
	if (!AppendToLog(logEntry)) {
		return false
	}
	votes := make(chan bool, len(cluster.Members) - 1)
	votesNeeded := (len(cluster.Members) / 2) // plus ourself to make a quorum
	for _, member := range cluster.Members {
		if (member != cluster.Self) {
			go SendAppendRpc(logEntry, member, votes)
		}
	}
	yesVotes := 1 // ourself
	noVotes := 0
	for {
		vote := <- votes
		if (vote) {
			yesVotes++
			votesNeeded--
		} else {
			noVotes++
		}
		if (votesNeeded == 0) {
			fmt.Printf("Successfully committed, append success\n")
			cluster.commitIndex = cluster.LastLogEntry
			return true
		}
		if (votesNeeded > (len(cluster.Members) - noVotes)) {
			fmt.Printf("Too many no votes, append fails\n")
			return false
		}
	}
}

func SaveStateToFile() {
	
}

func AppendToLog(entry *LogEntry) bool {
	cluster.clusterLock.Lock()
	defer cluster.clusterLock.Unlock()
	cluster.Log = append(cluster.Log, *entry)
	cluster.LastLogEntry++
	return CommitLog()
}

func followerAppendToLog(command *Command) {

}

func CommitLog() bool {
	return false
}