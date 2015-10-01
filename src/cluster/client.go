package cluster

import "fmt"
import "encoding/gob"
import "log"
import "os"
import "syscall"
import "time"

const REGISTER_PIPE = ".cluster/client.pipe"

var Pipe os.File

type Registration struct {
	Pipename string
}

func InitClient(pipename string) {
	if _, err := os.Stat(pipename); err == nil {
		fmt.Printf("pipe already exists\n")
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
    fmt.Printf("About to write to pipe: %s\n", pipename);
    time.Sleep(50*time.Millisecond)
	writePipe, err := os.OpenFile(pipename, os.O_WRONLY, 0666)
	if err != nil {
		fmt.Printf("Could not open command pipe (couldnt open %s for write)\n", pipename)
		log.Fatal(err)
	}
	testCmd := &Command{}
	testCmd.CType = GET
	testCmd.Key = "testKey"
	cmdEncoder := gob.NewEncoder(writePipe)
    err = cmdEncoder.Encode(testCmd)
	if (err != nil) {
    	fmt.Printf("Error writing %+v command pipe\n", testCmd)  
		log.Fatal(err)
    }
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





func ListenForClients(pipename string) {
	if _, err := os.Stat(pipename); err == nil {
		fmt.Printf("pipe already exists\n")
	} else {
		syscall.Mknod(pipename, syscall.S_IFIFO|0666, 0)
	}
	for {
		fmt.Printf("About to open pipe for read\n")
		readPipe, err := os.OpenFile(pipename, os.O_RDONLY, 0666)
		if err != nil {
			fmt.Printf("Could not open register pipe for read)\n", REGISTER_PIPE)
			log.Fatal(err)
		}
		defer readPipe.Close()
		msg := &Registration{}
		dataDecoder := gob.NewDecoder(readPipe)
		err = dataDecoder.Decode(msg)
		if err != nil {
			fmt.Printf("Error decoding registration msg\n");
			//log.Fatal(err)
			time.Sleep(10*time.Millisecond)
			continue
		}
		
		fmt.Printf("Need to open pipe for reading client commands: %+v\n", msg)
		go serveClient(msg.Pipename)
	}
}

func serveClient(pipename string) {
	if _, err := os.Stat(pipename); err == nil {
		fmt.Printf("pipe already exists\n")
	} else {
		syscall.Mknod(pipename, syscall.S_IFIFO|0666, 0)
	}
	for {
		readPipe, err := os.OpenFile(pipename, os.O_RDONLY, 0666)
		if err != nil {
			fmt.Printf("Could not open register pipe for read)\n", REGISTER_PIPE)
			log.Fatal(err)
		}
		defer readPipe.Close()
		msg := &Command{}
		dataDecoder := gob.NewDecoder(readPipe)
		err = dataDecoder.Decode(msg)
		if err != nil {
			fmt.Printf("Error decoding registration msg\n");
			//log.Fatal(err)
			time.Sleep(20*time.Millisecond)
			continue
		}
		fmt.Printf("Got command: %+v\n", msg)
	}
}
