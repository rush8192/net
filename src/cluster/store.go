package cluster

import "fmt"
import "io/ioutil"

var DB_DIR string = ".cluster/.db/"

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

