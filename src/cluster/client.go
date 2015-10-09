package cluster

import "fmt"
import "encoding/gob"
import "log"
import "os"
import "sync/atomic"
import "syscall"
import "time"

const REGISTER_PIPE = ".cluster/client.pipe"

var serialCommandId int64 = 0;
var clusterResponseByCommand map[int64]chan *Command = make(map[int64]chan *Command)

var pipeName string

type Registration struct {
	Pipename string
}

/*
 * Initializes the client module. After initialization, able to process GET,PUT,
 * UPDATE, DELETE, and COMMIT commands from a client program
 */
func InitClient(pipename string) {
	pipeName = pipename
	if _, err := os.Stat(getWritePipeName(pipename, true)); err == nil {
		fmt.Printf("write pipe already exists\n")
	} else {
		syscall.Mknod(getWritePipeName(pipename, true), syscall.S_IFIFO|0666, 0)
	}
	fmt.Printf("Registering client program\n")
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
    fmt.Printf("Starting program to listen for responses to commands\n")
    go ListenForCommandResponses(pipename)
    time.Sleep(50*time.Millisecond)
    fmt.Printf("Done initializing client\n")
}

/*
 * Listens to responses to outstanding client commands, routing them to the 
 * correct outstanding function
 */
func ListenForCommandResponses(pipename string) {
	if _, err := os.Stat(getReadPipeName(pipename, true)); err == nil {
		fmt.Printf("write pipe already exists\n")
	} else {
		syscall.Mknod(getReadPipeName(pipename,true), syscall.S_IFIFO|0666, 0)
	}
	for {
		readPipe, err := os.OpenFile(getReadPipeName(pipename,true), os.O_RDONLY, 0666)
		if err != nil {
			fmt.Printf("Could not open register pipe for read)\n", REGISTER_PIPE)
			log.Fatal(err)
		}
		defer readPipe.Close()
		msg := &Command{}
		dataDecoder := gob.NewDecoder(readPipe)
		err = dataDecoder.Decode(msg)
		if err != nil {
			fmt.Printf("Error decoding from command response pipe\n");
			//log.Fatal(err)
			time.Sleep(20*time.Millisecond)
			continue
		}
		fmt.Printf("Got response: %+v\n", msg)
		go routeCommandResponse(msg)
	}
}

func routeCommandResponse(response * Command) {
	cmdId := response.CId
	if (cmdId == 0) {
		fmt.Printf("Invalid command ID: %d\n", cmdId)
		return 
	}
	switch response.CType {
	case GET:
		fmt.Printf("Got response to get command: %s\n", response.Value)
		clusterResponseByCommand[cmdId] <- response
	case PUT:
		fmt.Printf("Got response to put command: %s\n", response.Key)
		clusterResponseByCommand[cmdId] <- response
	case UPDATE:
		
	case DELETE:
		
	default:
		fmt.Printf("Unrecognized command type %d\n", response.CType)
	}
}

func Put(key string, value []byte) string {
	fmt.Printf("Opening write pipe %s\n", getWritePipeName(pipeName, true))
	writePipe, err := os.OpenFile(getWritePipeName(pipeName, true), os.O_WRONLY, 0666)
	if err != nil {
		fmt.Printf("Could not open command pipe (couldnt open %s for write)\n", pipeName)
		log.Fatal(err)
	}
	fmt.Printf("Starting PUT command\n")
	putCmd := &Command{}
	putCmd.CType = PUT
	putCmd.Key = key
	putCmd.Value = value
	putCmd.CId = atomic.AddInt64(&serialCommandId, 1)
	
	responseChannel := make(chan *Command)
	clusterResponseByCommand[putCmd.CId] = responseChannel
	
	cmdEncoder := gob.NewEncoder(writePipe)
    err = cmdEncoder.Encode(putCmd)
	if (err != nil) {
    	fmt.Printf("Error writing %+v command pipe\n", putCmd)  
		log.Fatal(err)
    }
    writePipe.Close()
    fmt.Printf("Waiting for PUT response\n");
    return string((<- responseChannel).Key)
}

func Update(key string, value []byte) string {
	return key
}

func Delete(key string, value []byte) string {
	return key
}

func Get(key string) []byte {
	writePipe, err := os.OpenFile(getWritePipeName(pipeName, true), os.O_WRONLY, 0666)
	if err != nil {
		fmt.Printf("Could not open command pipe (couldnt open %s for write)\n", pipeName)
		log.Fatal(err)
	}
	fmt.Printf("Starting GET command\n")
	getCmd := &Command{}
	getCmd.CType = GET
	getCmd.Key = key
	getCmd.CId = atomic.AddInt64(&serialCommandId, 1)
	
	responseChannel := make(chan *Command)
	clusterResponseByCommand[getCmd.CId] = responseChannel
	
	cmdEncoder := gob.NewEncoder(writePipe)
    err = cmdEncoder.Encode(getCmd)
	if (err != nil) {
    	fmt.Printf("Error writing %+v command pipe\n", getCmd)  
		log.Fatal(err)
    }
    writePipe.Close()
    fmt.Printf("Waiting for GET response\n");
    return (<- responseChannel).Value
}

/********************************
 * Server side of client code
 */

func ListenForClients(pipename string) {
	if _, err := os.Stat(pipename); err == nil {
		fmt.Printf("pipe already exists\n")
	} else {
		syscall.Mknod(pipename, syscall.S_IFIFO|0666, 0)
	}
	servedClients := make(map[string]bool)
	for {
		fmt.Printf("About to open pipe %s for read\n", pipename)
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
		if (!servedClients[msg.Pipename]) {
			go serveClient(msg.Pipename)
		}
		servedClients[msg.Pipename] = true
	}
}

func serveClient(pipename string) {
	fmt.Printf("Opening pipe %s to receive client commands\n", getReadPipeName(pipename, false))
	if _, err := os.Stat(getReadPipeName(pipename, false)); err == nil {
		fmt.Printf("pipe already exists\n")
	} else {
		syscall.Mknod(getReadPipeName(pipename, false), syscall.S_IFIFO|0666, 0)
	}
	for {
		readPipe, err := os.OpenFile(getReadPipeName(pipename, false), os.O_RDONLY, 0666)
		if err != nil {
			fmt.Printf("Could not open client pipe for read)\n", getReadPipeName(pipename, false))
			//log.Fatal(err)
		}
		defer readPipe.Close()
		msg := &Command{}
		dataDecoder := gob.NewDecoder(readPipe)
		err = dataDecoder.Decode(msg)
		if err != nil {
			fmt.Printf("Error decoding client command msg\n");
			//log.Fatal(err)
			time.Sleep(20*time.Millisecond)
			continue
		}
		fmt.Printf("Got command: %+v\n", msg)
		AppendCommandToLog(msg)
		writePipe, err := os.OpenFile(getWritePipeName(pipename, false), os.O_WRONLY, 0666)
		if (err != nil) {
			fmt.Printf("Could not open client pipe %s for write)\n", getWritePipeName(pipename, false))
			//log.Fatal(err)
			continue
		}
		defer writePipe.Close()
		cmdEncoder := gob.NewEncoder(writePipe)
		err = cmdEncoder.Encode(msg)
		if (err != nil) {
			fmt.Printf("Error writing command response %+v command to pipe\n", msg)  
			//log.Fatal(err)
		}
	}
}


func getReadPipeName(pipeBase string, isClient bool) string {
	if (isClient) { 
		return pipeBase + "r"
	} else {
		return pipeBase + "w"
	}
}

func getWritePipeName(pipeBase string, isClient bool) string {
	if (isClient) {
		return pipeBase + "w"
	} else {
		return pipeBase + "r"
	}
} 
