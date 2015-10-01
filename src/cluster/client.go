package cluster

import "fmt"
import "encoding/gob"
import "log"
import "os"
import "syscall"

const REGISTER_PIPE = ".cluster/client.pipe"

var Pipe os.File

type Registration struct {
	pipename string
}

func InitClient(pipename string) {
	if _, err := os.Stat(pipename); err == nil {
		fmt.Printf("pipe already exists")
	} else {
		syscall.Mknod(pipename, syscall.S_IFIFO|0666, 0)
	}
	registerPipe, err := os.OpenFile(REGISTER_PIPE, os.O_WRONLY, 0666) // For write access.
	if err != nil {
		fmt.Printf("Could not register client with cluster program (couldnt open %s for write)\n", REGISTER_PIPE)
		log.Fatal(err)
	}
	msg := &Registration{ pipename }
	msgEncoder := gob.NewEncoder(registerPipe)
    err = msgEncoder.Encode(msg)
    if (err != nil) {
    	fmt.Printf("Error writing %+v register pipe\n", msg)  
		log.Fatal(err)
    }
	//writePipe := os.OpenFile(pipename, os.O_WR, 0666)
}

func create(key string, value []byte) string {
	//c := &Command{ GET, key, value}
	
	return key
}

func update(key string, value []byte) string {
	return key
}

func delete(key string, value []byte) string {
	return key
}

func get(key string) string {
	return key
}

func main() {
	InitClient("clienttest")
}

