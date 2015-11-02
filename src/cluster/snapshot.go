package cluster

import "os"
import "os/exec"
import "fmt"
import "time"

const IN_PROGRESS_SNAPSHOT = ".cluster/.tmpdbsnapshot/"
const SNAPSHOT_FOLDER = ".cluster/.dbsnapshot/"
const CURRENT_SNAPSHOT = ".cluster/.snapshot"

const SNAPSHOT_INTERVAL = 10000
const SNAPSHOT_THRESHOLD = 500

type InstallSnapshot struct {

}

type InstallSnapshotResponse struct {

}

type Snapshot struct {
	LastIncludedTerm int64
	LastIncludedIndex int64
	SnapshotFolder string
	Timestamp time.Time
}

func SnapshotSetTimer() {
	SnapshotStopTimer()
	if (VERBOSE > 1) {
		fmt.Printf("Setting snapshot timeout: %d\n", SNAPSHOT_INTERVAL)
	}
	cluster.snapshotTimer = time.AfterFunc(time.Duration(SNAPSHOT_INTERVAL)*time.Millisecond, TakeSnapshot)
}

func SnapshotStopTimer() bool {
	if (cluster.snapshotting) {
		return false
	}
	if (cluster.snapshotTimer != nil) {
		result := cluster.snapshotTimer.Stop()
		cluster.snapshotTimer = nil
		if (result == false) {
			return false
		}
	}
	return true
}

func SnapshotResetTimer() {
	if (cluster.Self.State != MEMBER && cluster.Self.State != LEADER) {
		return
	} 
	if (cluster.snapshotTimer != nil) {
		cluster.snapshotTimer.Stop()
	}
	SnapshotSetTimer()
}

func TakeSnapshot() {
	cluster.snapshotting = true
	cluster.clusterLock.Lock()
	defer cluster.clusterLock.Unlock()
	
	SnapshotStopTimer()
	snapshot := &Snapshot{ cluster.Log[cluster.LastLogEntry].Term, cluster.LastLogEntry,  
							SNAPSHOT_FOLDER, time.Now() }
	if (VERBOSE > 0) {
		fmt.Printf("Taking snapshot: %+v\n", snapshot)
	}
	// uses the cp command to take a snapshot of db directory
	cmd := "cp"
	args := []string{ "-r", DB_DIR, IN_PROGRESS_SNAPSHOT }
	if err := exec.Command(cmd, args...).Run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		
	} else {
		renameErr := os.Rename(IN_PROGRESS_SNAPSHOT, SNAPSHOT_FOLDER)
		if (renameErr != nil) {
			fmt.Fprintln(os.Stderr, renameErr) 
		} else {
			if (VERBOSE > 0) {
				fmt.Printf("Successfully took snapshot to %s\n", SNAPSHOT_FOLDER)
			}
		}
	}
	SnapshotResetTimer()
}

func SnapshotReplica(member *Node) {
	
}