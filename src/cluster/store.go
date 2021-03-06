package cluster

import "fmt"
import "io/ioutil"
import "os"

var DB_DIR string = ".cluster/db/"
var BACKUP_DB string = ".cluster/.db/"

const DEBUG_CLEAR_DB = true
const DEBUG_BACKUP_DB = true

/*
 * Initializes the backing folder used to keep track of present keys/values on disk
 */
func InitStore() {
	if (DEBUG_CLEAR_DB) {
		StoreClear()
	}
	os.MkdirAll(DB_DIR, 0777)
}

func StoreClear() {
	if (DEBUG_BACKUP_DB) {
		os.RemoveAll(BACKUP_DB)
		os.Rename(DB_DIR, BACKUP_DB)
	}
	os.RemoveAll(DB_DIR)
}

func StoreGet(key string) []byte {
	value, err := ioutil.ReadFile(DB_DIR + key)
	if (err != nil) {
		fmt.Println(err)
		return nil
	}
	return value
}

func StorePut(key string, value []byte) bool {
	err := ioutil.WriteFile(DB_DIR + key, value, 0666)
	if (err != nil) {
		fmt.Println(err)
		return false
	}
	return true
}

func StoreDelete(key string) bool {
	os.Remove(DB_DIR + key)
	if _, err := os.Stat(DB_DIR + key); err == nil {
		return false
	} else {
		return true
	}
}

