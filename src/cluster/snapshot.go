package cluster

import "archive/zip"
import "encoding/gob"
import "errors"
import "fmt"
import "io"
import "net"
import "os"
import "os/exec"
import "path/filepath"
import "strings"

import "strconv"
import "time"

const IN_PROGRESS_SNAPSHOT = ".cluster/.tmpdbsnapshot/"
const SNAPSHOT_FOLDER = ".cluster/dbsnapshot/"
const CURRENT_SNAPSHOT = ".cluster/snapshot"
const INCOMING_SNAPSHOT = ".cluster/.tmprpcsnapshot/"

const SNAPSHOT_INTERVAL = 10000
const SNAPSHOT_THRESHOLD = 500

const MAX_SNAPSHOT_BYTES = 500000

type InstallSnapshot struct {
	Term int64
	LeaderId string
	LastIncludedIndex int64
	LastIncludedTerm int64
	Offset int64
	Data []byte
	Done bool
}

type InstallSnapshotResponse struct {
	Success bool
	ResponseKey string
	Term int64
}

func getSnapshotListenKey(member *Node, snapshotRpc *InstallSnapshot) string {
	return member.Hostname + "|" + strconv.FormatInt(snapshotRpc.LastIncludedIndex, 10) + "|" + strconv.FormatInt(snapshotRpc.LastIncludedTerm, 10) + "|" + strconv.FormatInt(snapshotRpc.Offset, 10)
}

type Snapshot struct {
	LastIncludedTerm int64
	LastIncludedIndex int64
	SnapshotFolder string
	Timestamp time.Time
}

func SnapshotSetTimer() {
	SnapshotStopTimer()
	if (VERBOSE > 0) {
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
	args := []string{ "-rf", DB_DIR, IN_PROGRESS_SNAPSHOT }
	fmt.Printf("Executing %s with args: %v\n", cmd, args)
	if err := exec.Command(cmd, args...).Run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
	} else {
		if (VERBOSE > 0) {
			fmt.Printf("Renaming backup snapshot from %s to %s\n", IN_PROGRESS_SNAPSHOT, SNAPSHOT_FOLDER)
		}
		removeErr := os.RemoveAll(SNAPSHOT_FOLDER)
		if (removeErr != nil) {
			fmt.Fprintln(os.Stderr, removeErr) 
		} else {
			renameErr := os.Rename(IN_PROGRESS_SNAPSHOT, SNAPSHOT_FOLDER)
			if (renameErr != nil) {
				fmt.Fprintln(os.Stderr, renameErr) 
			} else {
				snapshotFile, err := os.Create(CURRENT_SNAPSHOT)
				if (err != nil) {
					snapshotFile, err = os.Create(CURRENT_SNAPSHOT) // retry
					if (err != nil) {
						fmt.Fprintln(os.Stderr, err) 
						SnapshotResetTimer()
						return
					}
				}
				defer snapshotFile.Close()
				encoder := gob.NewEncoder(snapshotFile)
				err = encoder.Encode(snapshot)
				if (err != nil) {
					fmt.Fprintln(os.Stderr, err) 
				} else {
					if (VERBOSE > 0) {
						fmt.Printf("Successfully took snapshot to %s\n", SNAPSHOT_FOLDER)
					}
				}
			}
		}
	}
	SnapshotResetTimer()
}

func unzip(archive, target string) error {
	reader, err := zip.OpenReader(archive)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(target, 0755); err != nil {
		return err
	}

	for _, file := range reader.File {
		path := filepath.Join(target, file.Name)
		if file.FileInfo().IsDir() {
			os.MkdirAll(path, file.Mode())
			continue
		}

		fileReader, err := file.Open()
		if err != nil {
			return err
		}
		defer fileReader.Close()

		targetFile, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, file.Mode())
		if err != nil {
			return err
		}
		defer targetFile.Close()

		if _, err := io.Copy(targetFile, fileReader); err != nil {
			return err
		}
	}
	return nil
}

func zipit(source string, target string) error {
	zipfile, err := os.Create(target)
	if err != nil {
		return err
	}
	defer zipfile.Close()

	archive := zip.NewWriter(zipfile)
	defer archive.Close()

	info, err := os.Stat(source)
	if err != nil {
		return nil
	}

	var baseDir string
	if info.IsDir() {
		baseDir = filepath.Base(source)
	}

	filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}

		if baseDir != "" {
			header.Name = filepath.Join(baseDir, strings.TrimPrefix(path, source))
		}

		if info.IsDir() {
			header.Name += string(os.PathSeparator)
		} else {
			header.Method = zip.Deflate
		}

		writer, err := archive.CreateHeader(header)
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		_, err = io.Copy(writer, file)
		return err
	})
	return err
}

func HandleInstallSnapshot(is *InstallSnapshot) {
	var snapshotFile *os.File
	var err error
	if (is.Offset == 0) {
		snapshotFile, err = os.Create(INCOMING_SNAPSHOT)
	} else {
		snapshotFile, err = os.OpenFile(INCOMING_SNAPSHOT, os.O_WRONLY, 0666)
	}
	if (err != nil) {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	defer snapshotFile.Close()
	toWrite := len(is.Data)
	for byteIndex := 0; byteIndex < toWrite; {
		bytesWritten, err := snapshotFile.WriteAt(is.Data, is.Offset + int64(byteIndex))
		if (err != nil) {
			fmt.Fprintln(os.Stderr, err)
			return
		}
		byteIndex += bytesWritten
	}
	if (is.Done) {
		finalizeSnapshot(INCOMING_SNAPSHOT)
	}
}

func finalizeSnapshot(tmpname string) {
	StoreClear()
	err := unzip(tmpname, DB_DIR)
	if (err != nil) {
		fmt.Fprintln(os.Stderr, err)
	} else {
		if (VERBOSE > 0) {
			fmt.Printf("Finalized snapshot to %s\n", DB_DIR)
		}
	}
}

func getSnapshotZipName(member *Node, snapshot *Snapshot) string {
	return "." + member.Hostname + "|" + strconv.FormatInt(snapshot.LastIncludedTerm, 10) + "|" + strconv.FormatInt(snapshot.LastIncludedIndex, 10)
}

/*
Term int64
	LeaderId string
	LastIncludedIndex int64
	LastIncludedTerm int64
	Offset int64
	Data []byte
	Done bool
*/

func HandleInstallSnapshotResponse(response *InstallSnapshotResponse) {
	if (response.ResponseKey == "" || response.Term > cluster.CurrentTerm) {
		return
	}
	cluster.rpcLock.Lock()
	channel, ok := cluster.oustandingRPC[response.ResponseKey]
	cluster.rpcLock.Unlock()
	if (ok) {
		channel <- response.Success
	}
	cluster.rpcLock.Lock()
	delete(cluster.oustandingRPC, response.ResponseKey)
	cluster.rpcLock.Unlock()
}

func SnapshotReplica(member *Node, retryRpc bool) error {
	cluster.clusterLock.Lock()
	SnapshotStopTimer()
	defer SnapshotSetTimer()
	snapshotFile, err := os.Open(CURRENT_SNAPSHOT)
	if (err != nil) {
		return err
	}
	snapshot := &Snapshot{}
	decoder := gob.NewDecoder(snapshotFile)
	err = decoder.Decode(snapshot)
	if (err != nil) {
		return err
	}
	
	var zipfile *os.File
	zipFilename := getSnapshotZipName(member, snapshot)
	err = zipit(SNAPSHOT_FOLDER, zipFilename)
	if (err != nil) {
		return err
	}
	
	cluster.clusterLock.Unlock()
	defer os.Remove(zipFilename)
	
	zipfile, err = os.Open(zipFilename)
	if (err != nil) {
		return err
	}
	filestat, err := zipfile.Stat()
	if (err != nil) {
		return err
	}
	fileSize := filestat.Size()
	for fileIndex := int64(0); fileIndex < fileSize; {
		bytesToRead := int64(MAX_SNAPSHOT_BYTES)
		if (fileIndex + MAX_SNAPSHOT_BYTES > fileSize) {
			bytesToRead = fileSize - fileIndex
		}
		rpcBytes := make([]byte, bytesToRead)
		bytesRead, err := zipfile.Read(rpcBytes)
		if (err != nil) {
			return err
		}
		done := false
		if (int64(bytesRead) + fileIndex == fileSize) {
			done = true
		}
		rpc := &InstallSnapshot{cluster.CurrentTerm, cluster.Self.Hostname,snapshot.LastIncludedIndex,
					snapshot.LastIncludedTerm, fileIndex, rpcBytes, done }
		err = sendInstallSnapshotRpc(member, rpc)
		if (err != nil && retryRpc) {
			err = sendInstallSnapshotRpc(member, rpc)
			if (err != nil) {
				return err
			}
		}
		fileIndex = fileIndex + int64(bytesRead)
	}
	return nil
}

func sendInstallSnapshotRpc(member *Node, rpc *InstallSnapshot) error {
	conn, err := net.Dial("tcp", member.Ip + ":" + CLUSTER_PORT)
	if err != nil {
		fmt.Printf("Connection error attempting to contact %s in sendInstallSnapshotRpc\n", member.Ip)
		return err
	}
	m := &Message{}
	m.MessageType = "InstallSnapshot"
	m.SnapshotRequest = rpc
	encoder := gob.NewEncoder(conn)
	err = encoder.Encode(m)
	if (err != nil) {
		fmt.Printf("Encode error attempting to respond to %s in sendInstallSnapshotRpc\n", member.Ip)
		return err
	} else {
		fmt.Printf(time.Now().String() + " Sent message: %+v to: %+v\n", m, member);
	}
	conn.Close()
	responseKey := getSnapshotListenKey(member, rpc)
	cluster.rpcLock.Lock()
	cluster.oustandingRPC[responseKey] = make(chan bool, 1)
	cluster.rpcLock.Unlock()
	success := <- cluster.oustandingRPC[responseKey]
	if (!success) {
		return errors.New("RPC returned false")
	}
	return nil
}