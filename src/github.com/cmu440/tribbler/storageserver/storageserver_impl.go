package storageserver

import (
	"errors"
    "time"
    "net"
    "net/rpc"
    "net/http"
    "sync"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type storageServer struct {
    isMaster    bool
    masterServerHostPort string

    userStorage map[string]userInfo
    userStorageLock &sync.Mutex

    masterRPC   *(rpc.Client)

    registerMutex   &sync.Mutex
    numRegistered   int
    nodes   []storagerpc.Node

    allRegistratedChan chan bool
}

type userInfo struct {
    // TODO: implement this!
    // store subscriptions, tribbles, etc.
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {

    // initialize the storageServer struct
	server := new(storageServer)
    if masterServerHostPort == "" {
        server.isMaster = true
    } else {
        server.masterServerHostPort = masterServerHostPort
    }
    server.userStorageLock = new(&sync.Mutex)
    server.registerMutex = new(&sync.Mutex)
    server.toRegister = numNodes
    numRegistered = 1 // include the master server

	// create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", strconv.Itoa(port))
	if err != nil {
		fmt.Printf("error while creating server socket for incoming RPCs\n")
		return nil, err
	}

	// Wrap the tribServer before registering it for RPC.
	err = rpc.RegisterName("StorageServer", storagerpc.Wrap(server))
	if err != nil {
		return nil, err
	}

	// Set up the HTTP handler that will serve incoming RPCs and
	// server requests in a background goroutine
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

    // if master, wait until all Nodes have registered
    if server.isMaster {
        nodes := new([]Node,numNodes)
            _ <- server.allRegistratedChan
        }

    // if slave, call RegisterServer every second until acknowledged
    } else {

        masterRPC, err = rpc.DialHTTP("tcp", masterServerHostPort)

        node := &Node{HostPort: "localhost:"+strconv.Itoa(port), NodeID:nodeID}
        args := &rpc.RegisterArgs{ServerInfo: node}
        reply := &rpc.RegisterReply{Status: rpc.NotReady}

        for {
            err := masterRPC.Call("StorageServer.RegisterServer", args, reply)
            if err != nil {
                return nil, err
            }
            if reply.Status == rpc.OK {
		        return server, nil
            }
		    _ <-time.After(time.Duration(1000) * time.Millisecond):
        }
    }
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {

    // register the node if it's new
    if ss.nodes[args.Node.NodeID] == nil {
        ss.nodes[args.Node.NodeID] = args.Node
        ss.registerMutex.Lock()
        ss.numRegistered++
        ss.registerMutex.Unlock()
    }

    // set status to OK if all nodes are registered
    if ss.numRegistered == ss.toRegister {
        reply.Status = storagerpc.OK
        ss.allRegisteredChan <- true
    }
    return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	return errors.New("not implemented")
}
