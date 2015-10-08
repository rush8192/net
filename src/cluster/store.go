package cluster

import "fmt"
import "io/ioutil"
import "os"

var DB_DIR string = ".cluster/.db/"

func InitStore() {
	os.MkdirAll(DB_DIR, 0777)
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

