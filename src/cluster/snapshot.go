package cluster

import "time"

const IN_PROGRESS_SNAPSHOT = ".cluster/.tmpdbsnapshot/"
const SNAPSHOT_FOLDER = ".cluster/.dbsnapshot/"
const CURRENT_SNAPSHOT = ".cluster/.snapshot"

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