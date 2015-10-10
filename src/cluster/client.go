package cluster

import "fmt"
import "encoding/gob"
import "log"
import "os"
import "sync"
import "sync/atomic"
import "syscall"
import "time"

const REGISTER_PIPE = ".cluster/client.pipe"

type Registration struct {
	ClientName string
}

type Client struct {
	name string
	pipeLock *sync.Mutex
	clusterResponseByCommand map[int64]chan *Command
	cmdRouteLock *sync.RWMutex
	serialCommandId int64
}

/*
 * Initializes the client module. After initialization, able to process GET,PUT,
 * UPDATE, DELETE, and COMMIT commands from a client program
 */
func InitClient(clientName string) *Client {
	client := &Client{ clientName, &sync.Mutex{}, make(map[int64]chan *Command), &sync.RWMutex{}, 1 }
	RegisterClient(client)
    go ListenForCommandResponses(client)
    time.Sleep(150*time.Millisecond)
    return client
}

func RegisterClient(client *Client) {
	if _, err := os.Stat(getWritePipeName(client.name, true)); err == nil {
		fmt.Printf("write pipe already exists\n")
	} else {
		syscall.Mknod(getWritePipeName(client.name, true), syscall.S_IFIFO|0666, 0)
	}
	registerPipe, err := os.OpenFile(REGISTER_PIPE, os.O_WRONLY, 0666) // For write access.
	if err != nil {
		fmt.Printf("Could not register client with cluster program (couldnt open %s for write)\n", REGISTER_PIPE)
		log.Fatal(err)
	}
	msg := &Registration{ client.name }
	msgEncoder := gob.NewEncoder(registerPipe)
    err = msgEncoder.Encode(msg)
    if (err != nil) {
    	fmt.Printf("Error writing %+v register pipe\n", msg)  
		log.Fatal(err)
    }
}

/*
 * Listens to responses to outstanding client commands, routing them to the 
 * correct outstanding function
 */
func ListenForCommandResponses(client *Client) {
	if _, err := os.Stat(getReadPipeName(client.name, true)); err == nil {
		fmt.Printf("write pipe already exists\n")
	} else {
		syscall.Mknod(getReadPipeName(client.name, true), syscall.S_IFIFO|0666, 0)
	}
	for {
		readPipe, err := os.OpenFile(getReadPipeName(client.name, true), os.O_RDONLY, 0666)
		if err != nil {
			fmt.Printf("Could not open pipe %s for read)\n", getReadPipeName(client.name, true))
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
		go routeCommandResponse(client, msg)
	}
}

func routeCommandResponse(client *Client, response * Command) {
	cmdId := response.CId
	if (cmdId == 0) {
		fmt.Printf("Invalid command ID: %d\n", cmdId)
		return 
	}
	client.cmdRouteLock.RLock()
	switch response.CType {
	case GET:
		fmt.Printf("Got response to get command: %s\n", response.Value)
		client.clusterResponseByCommand[cmdId] <- response
	case PUT:
		fmt.Printf("Got response to put command: %s\n", response.Key)
		client.clusterResponseByCommand[cmdId] <- response
		fmt.Printf("Sent response to channel %d\n", cmdId)
	case UPDATE:
		fmt.Printf("Got response to update command: %s\n", response.Key)
		client.clusterResponseByCommand[cmdId] <- response
	case DELETE:
		fmt.Printf("Got response to delete command: %s\n", response.Key)
		client.clusterResponseByCommand[cmdId] <- response
	default:
		fmt.Printf("Unrecognized command type %d\n", response.CType)
	}
	client.cmdRouteLock.RUnlock()
	client.cmdRouteLock.Lock()
	delete(client.clusterResponseByCommand, cmdId)
	client.cmdRouteLock.Unlock()
}

func (client *Client) Put(key string, value []byte) string {
	fmt.Printf("Opening write pipe %s to put %s\n", getWritePipeName(client.name, true), key)
	client.pipeLock.Lock()
	writePipe, err := os.OpenFile(getWritePipeName(client.name, true), os.O_WRONLY, 0666)
	if err != nil {
		fmt.Printf("Could not open command pipe (couldnt open %s for write)\n", client.name)
		log.Fatal(err)
	}
	fmt.Printf("Starting PUT command for %s\n", key)
	putCmd := &Command{}
	putCmd.CType = PUT
	putCmd.Key = key
	putCmd.Value = value
	putCmd.CId = atomic.AddInt64(&client.serialCommandId, 1)
	
	responseChannel := make(chan *Command)
	client.cmdRouteLock.Lock()
	fmt.Printf("Listening for PUT response for %s at channel %d\n", key, putCmd.CId)
	client.clusterResponseByCommand[putCmd.CId] = responseChannel
	client.cmdRouteLock.Unlock()
	cmdEncoder := gob.NewEncoder(writePipe)
    err = cmdEncoder.Encode(putCmd)
	if (err != nil) {
    	fmt.Printf("Error writing %+v to command pipe\n", putCmd)  
		log.Fatal(err)
    }
    writePipe.Close()
    client.pipeLock.Unlock()
    fmt.Printf("Waiting for PUT response\n");
    return string((<- responseChannel).Key)
}

func (client *Client) Update(key string, value []byte) string {
	fmt.Printf("Opening write pipe %s\n", getWritePipeName(client.name, true))
	client.pipeLock.Lock()
	writePipe, err := os.OpenFile(getWritePipeName(client.name, true), os.O_WRONLY, 0666)
	if err != nil {
		fmt.Printf("Could not open command pipe (couldnt open %s for write)\n", client.name)
		log.Fatal(err)
	}
	fmt.Printf("Starting UPDATE command\n")
	putCmd := &Command{}
	putCmd.CType = UPDATE
	putCmd.Key = key
	putCmd.Value = value
	putCmd.CId = atomic.AddInt64(&client.serialCommandId, 1)
	
	responseChannel := make(chan *Command)
	client.cmdRouteLock.Lock()
	client.clusterResponseByCommand[putCmd.CId] = responseChannel
	client.cmdRouteLock.Unlock()
	cmdEncoder := gob.NewEncoder(writePipe)
    err = cmdEncoder.Encode(putCmd)
	if (err != nil) {
    	fmt.Printf("Error writing %+v to command pipe\n", putCmd)  
		log.Fatal(err)
    }
    writePipe.Close()
    client.pipeLock.Unlock()
    fmt.Printf("Waiting for UPDATE response\n");
    return string((<- responseChannel).Key)
}

func (client *Client) Delete(key string) string {
	client.pipeLock.Lock()
	writePipe, err := os.OpenFile(getWritePipeName(client.name, true), os.O_WRONLY, 0666)
	if err != nil {
		fmt.Printf("Could not open command pipe (couldnt open %s for write)\n", client.name)
		log.Fatal(err)
	}
	fmt.Printf("Starting DELETE command\n")
	getCmd := &Command{}
	getCmd.CType = DELETE
	getCmd.Key = key
	getCmd.CId = atomic.AddInt64(&client.serialCommandId, 1)
	
	responseChannel := make(chan *Command)
	client.cmdRouteLock.Lock()
	client.clusterResponseByCommand[getCmd.CId] = responseChannel
	client.cmdRouteLock.Unlock()
	cmdEncoder := gob.NewEncoder(writePipe)
    err = cmdEncoder.Encode(getCmd)
	if (err != nil) {
    	fmt.Printf("Error writing %+v command pipe\n", getCmd)  
		log.Fatal(err)
    }
    writePipe.Close()
    fmt.Printf("Waiting for DELETE response\n");
    return (<- responseChannel).Key
}

func (client *Client) Get(key string) []byte {
	client.pipeLock.Lock()
	writePipe, err := os.OpenFile(getWritePipeName(client.name, true), os.O_WRONLY, 0666)
	if err != nil {
		fmt.Printf("Could not open command pipe (couldnt open %s for write)\n", client.name)
		log.Fatal(err)
	}
	fmt.Printf("Starting GET command\n")
	getCmd := &Command{}
	getCmd.CType = GET
	getCmd.Key = key
	getCmd.CId = atomic.AddInt64(&client.serialCommandId, 1)
	
	responseChannel := make(chan *Command)
	client.cmdRouteLock.Lock()
	client.clusterResponseByCommand[getCmd.CId] = responseChannel
	client.cmdRouteLock.Unlock()
	cmdEncoder := gob.NewEncoder(writePipe)
    err = cmdEncoder.Encode(getCmd)
	if (err != nil) {
    	fmt.Printf("Error writing %+v command pipe\n", getCmd)  
		log.Fatal(err)
    }
    writePipe.Close()
    fmt.Printf("### %s: Sent command: %+v\n", time.Now().String(), getCmd)
    client.pipeLock.Unlock()
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
			fmt.Printf("Error decoding registration msg from %s\n", pipename);
			//log.Fatal(err)
			time.Sleep(10*time.Millisecond)
			continue
		}
		
		fmt.Printf("Need to open pipe for reading client commands: %+v\n", msg)
		if (!servedClients[msg.ClientName]) {
			go serveClient(msg.ClientName)
		}
		servedClients[msg.ClientName] = true
	}
}

func serveClient(clientName string) {
	fmt.Printf("Opening pipe %s to receive client commands\n", getReadPipeName(clientName, false))
	if _, err := os.Stat(getReadPipeName(clientName, false)); err == nil {
		fmt.Printf("pipe already exists\n")
	} else {
		syscall.Mknod(getReadPipeName(clientName, false), syscall.S_IFIFO|0666, 0)
	}
	responseLock := &sync.Mutex{}
	for {
		readPipe, err := os.OpenFile(getReadPipeName(clientName, false), os.O_RDONLY, 0666)
		if err != nil {
			fmt.Printf("Could not open client pipe for read)\n", getReadPipeName(clientName, false))
			continue
		}
		defer readPipe.Close()
		msg := &Command{}
		dataDecoder := gob.NewDecoder(readPipe)
		err = dataDecoder.Decode(msg)
		if err != nil {
			fmt.Printf("Error decoding client command msg\n");
			time.Sleep(20*time.Millisecond)
			continue
		}
		fmt.Printf("### %s: Got command: %+v\n", time.Now().String(), msg)
		go handleClientCommand(clientName, msg, responseLock)
	}
}

func handleClientCommand(clientName string, cmd *Command, responseLock *sync.Mutex) {
	AppendCommandToLog(cmd)
	responseLock.Lock()
	defer responseLock.Unlock()
	writePipe, err := os.OpenFile(getWritePipeName(clientName, false), os.O_WRONLY, 0666)
	if (err != nil) {
		fmt.Printf("Could not open client pipe %s for write)\n", getWritePipeName(clientName, false))
		return
	}
	defer writePipe.Close()
	cmdEncoder := gob.NewEncoder(writePipe)
	err = cmdEncoder.Encode(cmd)
	if (err != nil) {
		fmt.Printf("Error writing command response %+v command to pipe\n", cmd)  
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
