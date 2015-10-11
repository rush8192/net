package cluster

import "errors"
import "fmt"
import "encoding/gob"
import "log"
import "net"
import "os"
import "os/signal"
import "sync"
import "sync/atomic"
import "syscall"
import "time"

const REGISTER_PIPE = ".cluster/client.pipe"
const TIMEOUT_MS = 10000

type Registration struct {
	ClientName string
}

type Client struct {
	Name string
	PipeLock *sync.Mutex
	ClusterResponseByCommand map[int64]chan *Command
	CmdRouteLock *sync.RWMutex
	SerialCommandId int64
	WritePipe *os.File
	CListener net.Listener
}

/*
 * Initializes the client module. After initialization, able to process GET,PUT,
 * UPDATE, DELETE, and COMMIT commands from a client program
 */
func InitClient(clientName string) *Client {
	client := &Client{ clientName, &sync.Mutex{}, make(map[int64]chan *Command), &sync.RWMutex{}, 1 ,nil, nil}
	RegisterClient(client)
	done := make(chan bool)
    go ListenForCommandResponses(client, done)
    <- done
    fmt.Printf("Done initializing client\n");
    return client
}

func (client *Client) Exit() {
	if (client.CListener != nil) {
		client.CListener.Close()
	}
}

func RegisterClient(client *Client) {
	fmt.Printf("Registering client %s\n", client.Name)
	registerPipe, err := os.OpenFile(REGISTER_PIPE, os.O_WRONLY, 0666) // For write access.
	if err != nil {
		fmt.Printf("Could not register client with cluster program (couldnt open %s for write)\n", REGISTER_PIPE)
		log.Fatal(err)
	}
	fmt.Printf("Sending registration\n")
	msg := &Registration{ client.Name }
	msgEncoder := gob.NewEncoder(registerPipe)
    err = msgEncoder.Encode(msg)
    if (err != nil) {
    	fmt.Printf("Error writing %+v register pipe\n", msg)  
		log.Fatal(err)
		client.Exit()
    }
    fmt.Printf("Done registering\n")
}

/*
 * Listens to responses to outstanding client commands, routing them to the 
 * correct outstanding function
 */
func ListenForCommandResponses(client *Client, done chan bool) {
	readSocket, err := net.Listen("unix", GetReadPipeName(client.Name, true))
    if err != nil {
        fmt.Printf("Could not open client socket %s for read: %s\n", GetReadPipeName(client.Name,  true), err.Error())
        done <- false
        return
    }
    client.CListener = readSocket
    sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, os.Kill, syscall.SIGTERM)
    go handleSignal(sigc, readSocket)
	defer readSocket.Close()
	fmt.Printf("Waiting for command responses\n")
	done <- true
	for {
		conn, err := readSocket.Accept()
        if err != nil {
            fmt.Println("accept error", err.Error())
            return
        }
		msg := &Command{}
		dataDecoder := gob.NewDecoder(conn)
		err = dataDecoder.Decode(msg)
		conn.Close()
		if err != nil {
			fmt.Printf("Error decoding from command response pipe\n");
			//log.Fatal(err)
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
	client.CmdRouteLock.RLock()
	switch response.CType {
	case GET:
		fmt.Printf("Got response to get command: %s\n", response.Value)
		client.ClusterResponseByCommand[cmdId] <- response
	case PUT:
		fmt.Printf("Got response to put command: %s\n", response.Key)
		client.ClusterResponseByCommand[cmdId] <- response
		fmt.Printf("Sent response to channel %d\n", cmdId)
	case UPDATE:
		fmt.Printf("Got response to update command: %s\n", response.Key)
		client.ClusterResponseByCommand[cmdId] <- response
	case DELETE:
		fmt.Printf("Got response to delete command: %s\n", response.Key)
		client.ClusterResponseByCommand[cmdId] <- response
	case FAILED:
		fmt.Printf("  FAILED command %+v\n", response)
		client.ClusterResponseByCommand[cmdId] <- response
	default:
		fmt.Printf("Unrecognized command type %d\n", response.CType)
	}
	client.CmdRouteLock.RUnlock()
	client.CmdRouteLock.Lock()
	delete(client.ClusterResponseByCommand, cmdId)
	client.CmdRouteLock.Unlock()
}

func (client *Client) Put(key string, value []byte) (string, error) {
	fmt.Printf("Starting PUT command for %s\n", key)
	putCmd := &Command{}
	putCmd.CType = PUT
	putCmd.Key = key
	putCmd.Value = value
	putCmd.CId = atomic.AddInt64(&client.SerialCommandId, 1)
	
	responseChannel := make(chan *Command)
	client.CmdRouteLock.Lock()
	fmt.Printf("Listening for PUT response for %s at channel %d\n", key, putCmd.CId)
	client.ClusterResponseByCommand[putCmd.CId] = responseChannel
	client.CmdRouteLock.Unlock()
	client.PipeLock.Lock()
	conn, err := net.Dial("unix", GetWritePipeName(client.Name, true))
	if err != nil {
		fmt.Printf("Connection error attempting to dial socket %s \n", GetWritePipeName(client.Name, true))
		client.Exit()
		log.Fatal(err)
	}
	cmdEncoder := gob.NewEncoder(conn)
    err = cmdEncoder.Encode(putCmd)
    conn.Close()
	if (err != nil) {
    	fmt.Printf("Error writing %+v to command pipe\n", putCmd)  
		client.Exit()
		log.Fatal(err)
    }
    fmt.Printf("### %s: Sent command: %+v\n", time.Now().String(), putCmd)
    client.PipeLock.Unlock()
    fmt.Printf("Waiting for PUT response\n");
    response := (<- responseChannel)
    err = nil
    if (response.CType == FAILED) {
    	err = errors.New("Get Failed")
    }
    return string(response.Key), err
}

func (client *Client) Update(key string, value []byte) (string, error) {
	fmt.Printf("Opening write pipe %s\n", GetWritePipeName(client.Name, true))
	
	fmt.Printf("Starting UPDATE command\n")
	putCmd := &Command{}
	putCmd.CType = UPDATE
	putCmd.Key = key
	putCmd.Value = value
	putCmd.CId = atomic.AddInt64(&client.SerialCommandId, 1)
	
	responseChannel := make(chan *Command)
	client.CmdRouteLock.Lock()
	client.ClusterResponseByCommand[putCmd.CId] = responseChannel
	client.CmdRouteLock.Unlock()
	client.PipeLock.Lock()
	conn, err := net.Dial("unix", GetWritePipeName(client.Name, true))
	if err != nil {
		fmt.Printf("Connection error attempting to dial socket %s \n", GetWritePipeName(client.Name, true))
		client.Exit()
		log.Fatal(err)
	}
	defer conn.Close()
	cmdEncoder := gob.NewEncoder(conn)
    err = cmdEncoder.Encode(putCmd)
	if (err != nil) {
    	fmt.Printf("Error writing %+v to command pipe\n", putCmd)
    	client.Exit()  
		log.Fatal(err)
    }
    client.PipeLock.Unlock()
    fmt.Printf("Waiting for UPDATE response\n");
    response := (<- responseChannel)
    err = nil
    if (response.CType == FAILED) {
    	err = errors.New("UPDATE Failed")
    }
    return string(response.Key), err
}

func (client *Client) Delete(key string) (string, error) {
	fmt.Printf("Starting DELETE command\n")
	dltCmd := &Command{}
	dltCmd.CType = DELETE
	dltCmd.Key = key
	dltCmd.CId = atomic.AddInt64(&client.SerialCommandId, 1)
	
	responseChannel := make(chan *Command)
	client.CmdRouteLock.Lock()
	client.ClusterResponseByCommand[dltCmd.CId] = responseChannel
	client.CmdRouteLock.Unlock()
	client.PipeLock.Lock()
	conn, err := net.Dial("unix", GetWritePipeName(client.Name, true))
	if err != nil {
		fmt.Printf("Connection error attempting to dial socket %s \n", GetWritePipeName(client.Name, true))
		client.Exit()
		log.Fatal(err)
	}
	cmdEncoder := gob.NewEncoder(conn)
    err = cmdEncoder.Encode(dltCmd)
	if (err != nil) {
    	fmt.Printf("Error writing %+v command pipe\n", dltCmd)  
    	client.Exit()
		log.Fatal(err)
    }
    conn.Close()
    client.PipeLock.Unlock()
    fmt.Printf("Waiting for DELETE response\n");
    response := (<- responseChannel)
    err = nil
    if (response.CType == FAILED) {
    	err = errors.New("DELETE Failed")
    }
    return response.Key, err
}

func (client *Client) Get(key string) ([]byte, error) {
	fmt.Printf("Starting GET command\n")
	getCmd := &Command{}
	getCmd.CType = GET
	getCmd.Key = key
	getCmd.CId = atomic.AddInt64(&client.SerialCommandId, 1)
	
	responseChannel := make(chan *Command)
	client.CmdRouteLock.Lock()
	client.ClusterResponseByCommand[getCmd.CId] = responseChannel
	client.CmdRouteLock.Unlock()
	client.PipeLock.Lock()
	conn, err := net.Dial("unix", GetWritePipeName(client.Name, true))
	if err != nil {
		fmt.Printf("Connection error attempting to dial socket %s \n", GetWritePipeName(client.Name, true))
		client.Exit()
		log.Fatal(err)
	}
	cmdEncoder := gob.NewEncoder(conn)
    err = cmdEncoder.Encode(getCmd)
	if (err != nil) {
    	fmt.Printf("Error writing %+v command pipe\n", getCmd)  
		client.Exit()
		log.Fatal(err)
    }
    conn.Close()
    fmt.Printf("### %s: Sent command: %+v\n", time.Now().String(), getCmd)
    client.PipeLock.Unlock()
    fmt.Printf("Waiting for GET response\n");
    response := (<- responseChannel)
    err = nil
    if (response.CType == FAILED) {
    	err = errors.New("Get Failed")
    }
    return response.Value, err
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
	fmt.Printf("About to open pipe %s for read\n", pipename)
	readPipe, err := os.OpenFile(pipename, os.O_RDONLY, 0666)
	if err != nil {
		fmt.Printf("Could not open register pipe for read)\n", REGISTER_PIPE)
		log.Fatal(err)
	}
	defer readPipe.Close()
	for {
		msg := &Registration{}
		dataDecoder := gob.NewDecoder(readPipe)
		err = dataDecoder.Decode(msg)
		if err != nil {
			fmt.Printf("Error decoding registration msg from %s\n", pipename);
			//log.Fatal(err)
			readPipe.Close()
			time.Sleep(10*time.Millisecond)
			readPipe, err = os.OpenFile(pipename, os.O_RDONLY, 0666)
			if err != nil {
				fmt.Printf("Could not open register pipe for read)\n", REGISTER_PIPE)
				log.Fatal(err)
			}
		}
		
		fmt.Printf("Need to open pipe for reading client commands: %+v\n", msg)
		if (!servedClients[msg.ClientName]) {
			go serveClient(msg.ClientName, servedClients)
		}
		servedClients[msg.ClientName] = true
	}
}

func serveClient(clientName string, servedClients map[string]bool) {
	responseLock := &sync.Mutex{}
	fmt.Printf("Serving client on socket %s\n", GetReadPipeName(clientName, false))
	readSocket, err := net.Listen("unix", GetReadPipeName(clientName, false))
    if err != nil {
        fmt.Printf("Could not open client socket %s for read: %s\n", GetReadPipeName(clientName,  false), err.Error())
        log.Fatal(err)
    }
    sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, os.Kill, syscall.SIGTERM)
    go handleSignal(sigc, readSocket)
    defer readSocket.Close()
	for {
		conn, err := readSocket.Accept()
        if err != nil {
            fmt.Printf("Error accepting client command connection\n");
			fmt.Printf(err.Error())
			delete(servedClients, clientName)
			return
        }
        msg := &Command{}
		dec := gob.NewDecoder(conn)
		err = dec.Decode(msg)
		if err != nil {
			fmt.Printf("Error decoding client command msg\n");
			fmt.Printf(err.Error())
			delete(servedClients, clientName)
			return
		}
		conn.Close()
		fmt.Printf("### %s: Got command: %+v\n", time.Now().String(), msg)
		go handleClientCommand(clientName, msg, GetWritePipeName(clientName, false), responseLock)
	}
}

func handleClientCommand(clientName string, cmd *Command, responseSocket string, responseLock *sync.Mutex) {	
	AppendCommandToLog(cmd)
	responseLock.Lock()
	defer responseLock.Unlock()
	conn, err := net.Dial("unix", responseSocket)
	if err != nil {
		fmt.Printf("Connection error attempting to dial socket %s \n", responseSocket)
		return
	}
	defer conn.Close()
	encoder := gob.NewEncoder(conn)
	err = encoder.Encode(cmd)
	if (err != nil) {
		fmt.Printf("Encode error attempting to send Message to %s\n", responseSocket)
		log.Fatal(err)
	}
}

func handleSignal(c chan os.Signal, l net.Listener) {
	sig := <-c
    log.Printf("Caught signal %s: shutting down.", sig)
    // Stop listening (and unlink the socket if unix type):
    l.Close()
    os.Exit(0)
}

func GetReadPipeName(pipeBase string, isClient bool) string {
	if (isClient) { 
		return ".cluster/" + pipeBase + "r"
	} else {
		return ".cluster/" + pipeBase + "w"
	}
}

func GetWritePipeName(pipeBase string, isClient bool) string {
	if (isClient) {
		return ".cluster/" + pipeBase + "w"
	} else {
		return ".cluster/" + pipeBase + "r"
	}
} 
