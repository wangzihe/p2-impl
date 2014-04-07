package storageserver

import (
	"container/list"
	"errors"
    "time"
    "net"
    "net/rpc"
    "net/http"
    "sync"
    "hash/fnv"

	"github.com/cmu440/tribbler/rpc/storagerpc"
	"github.com/cmu440/tribbler/libstore"
)

type storageServer struct {

    // variables used for registration
    registerMutex   &sync.Mutex
    numRegistered   int
    allRegistratedChan chan bool
    numRegistered   bool

    // hash ring info
    nodes   []storagerpc.Node
    minKey, maxKey int

    // primary data structures
    simpleStore   map[string]string
    listStore     map[string]([]string)

    // locks for primary data structures; also lock protector maps
    simpleLock, listLock    &sync.Mutex

    // locks for each element of primary data structures
    simpleProtectors, listProtectors    map[string]protector

    libstoreRPCs    *(rpc.Client)
}

type owner struct {
    leaseEnd    time.Duration
    HostPort    string
    RPC
}

type protector struct {
    leaseOwners     *list.List
    beingModified   bool //bM
    storeLock, bMLock &sync.Mutex
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
    server.registerMutex = new(&sync.Mutex)
    server.toRegister = numNodes
    server.numRegistered = 1 // include the master server
    server.maxKey = nodeID
    node := &Node{HostPort: "localhost:"+strconv.Itoa(port), NodeID:nodeID}

    server.simpleLock = new(&sync.Mutex)
    server.listLock = new(&sync.Mutex)
    server.simpleStore = make(map[string]string)
    server.listStore = make(map[string]([]string))
    simpleProtectors = make(map[string]protector)
    listProtectors = make(map[string]protector)

	// create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", strconv.Itoa(port))
	if err != nil {
		fmt.Printf("error while creating server socket for incoming RPCs\n")
		return nil, err
	}

	// Wrap the storageServer before registering it for RPC.
	err = rpc.RegisterName("StorageServer", storagerpc.Wrap(server))
	if err != nil {
		return nil, err
	}

	// Set up the HTTP handler that will serve incoming RPCs and
	// server requests in a background goroutine
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

    // if master, wait until all Nodes have registered
    if masterServerHostPort == "" {
        server.nodes := new([]Node,numNodes)
        server.nodes[0] = node // register master as first node
        _ <- server.allRegistratedChan // wait until all nodes are registered
        server.minKey = prevNodeID(nodeID, nodes)
        return server, nil

    // if slave, call RegisterServer every second until acknowledged
    } else {

        // prepare to call RegisterServer
        masterRPC, err := rpc.DialHTTP("tcp", masterServerHostPort)
        args := &rpc.RegisterArgs{ServerInfo: node}
        reply := &rpc.RegisterReply{Status: rpc.NotReady}

        for {
            err := masterRPC.Call("StorageServer.RegisterServer", args, reply)
            if err != nil {
                return nil, err
            }
            if reply.Status == rpc.OK {
                server.nodes = reply.Servers
                server.minKey = prevNodeID(nodeID, server.nodes)
		        return server, nil
            }
		    _ <-time.After(time.Duration(1000) * time.Millisecond):
        }
    }
}

// helper function which returns the ID of the node whose ID immediately
// precedes nodeID in the hashing ring
func prevNodeID(nodeID uint32, nodes []Node) uint32 {

    // check if node has smallest hash
    isMin := true
    for i = 0; i < numNodes; i++ {
        if nodes[i].NodeID < nodeID {
            isMin = false
            break
        }
    }

    // find the preceding node in the hash ring
    minKey := 0
    if isMin { // get max key
        for i = 0; i < numNodes; i++ {
            if nodes[i].NodeID > minKey {
                minKey = NodeID
            }
        }
    } else { // get max key BEFORE nodeID
        for i = 0; i < numNodes; i++ {
            if nodes[i].NodeID > minKey && nodes[i].NodeID < maxKey {
                minKey = nodes[i].NodeID
            }
        }
    }
    return minKey
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {

    unRegistered := true
    ss.registerMutex.Lock()
    for i := 0; i < ss.numRegistered; i++ {
        unRegistered = unRegistered && (args.Node.NodeID != ss.nodes[i].NodeID)
    }

    // register the node if it's new
    if unRegistered == nil {
        ss.nodes[ss.numRegistered] = args.Node
        ss.numRegistered++
    }
    ss.registerMutex.Unlock()

    // set status to OK and notify main goroutine if all nodes are registered
    if ss.numRegistered == ss.toRegister {
        reply.Status = storagerpc.OK
        ss.allRegisteredChan <- true
    } else {
        reply.Status = storagerpc.NotReady
    }
    return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
    if ss.numRegistered == ss.toRegister {
        reply.Status = storagerpc.OK
        reply.Servers = ss.nodes
    } else {
        reply.Status = storagerpc.NotReady
    }
    return nil
}

// helper function which returns true if hash is in the range (minKey, maxKey],
// accounting for uint32 wrapping
func inRange(hash, minKey, maxKey uint32) bool {
    return (hash > minkey && hash <= maxKey) ||
        (minKey >= maxKey && (hash > minKey || hash <= maxKey))
}

// Get retrieves the specified key from the data store and replies with
// the key's value and a lease if one was requested. If the key does not
// fall within the storage server's range, it should reply with status
// WrongServer. If the key is not found, it should reply with status
// KeyNotFound.
func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {

    // check that the key is in the server's hash range
    keyHash := libstore.StoreHash(strings.Split(args.Key,":")[0])
    if !inRange(keyHash, ss.minKey, ss.maxKey) {
        reply.Status = storagerpc.WrongServer
        return nil
    }

    // check that the key exists
    ss.simpleStoreLock.Lock()
    if val, pres := ss.simpleStore[args.Key]; !pres {
        ss.simpleStoreLock.Unlock()
        reply.Status = storagerpc.KeyNotFound
        return nil
    }

    // handle leasing, if requested
    if args.WantLease {

        prot := simpleProtectors[args.Key]
        prot.bMLock.Lock()
        ss.simpleStoreLock.Unlock()

        if prot.beingModified { // reject lease request
            reply.Lease = &storagerpc.Lease{Granted: false}

        } else {    // grant lease
            // prepare reply
            duration := time.Duration(LeaseSeconds) * time.Second
            reply.Lease = &storagerpc.Lease{Granted: true, Duration: duration}

            // record lease
            o := &owner{leaseEnd: time.Now().Add(duration)}
            o.HostPort = args.HostPort
            prot.leaseOwners.PushBack(o)
        }
        prot.bMLock.Unlock()
    } else {
        ss.simpleStoreLock.Unlock()
    }

    reply.Status = storagerpc.OK
    reply.Value = val
	return nil
}

// GetList retrieves the specified key from the data store and replies with
// the key's list value and a lease if one was requested. If the key does not
// fall within the storage server's range, it should reply with status
// WrongServer. If the key is not found, it should reply with status
// KeyNotFound.
func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {

    // check that the key is in the server's hash range
    keyHash := libstore.StoreHash(strings.Split(args.Key,":")[0])
    if !inRange(keyHash, ss.minKey, ss.maxKey) {
        reply.Status = storagerpc.WrongServer
        return nil
    }

    // check that the key exists
    ss.listStoreLock.Lock()
    if val, pres := ss.listStore[args.Key]; !pres {
        ss.listStoreLock.Unlock()
        reply.Status = storagerpc.KeyNotFound
        return nil
    }

    // handle leasing, if requested
    if args.WantLease {

        prot := listProtectors[args.Key]
        prot.bMLock.Lock()
        ss.listStoreLock.Unlock()

        if prot.beingModified { // reject lease request
            reply.Lease = &storagerpc.Lease{Granted: false}

        } else {    // grant lease
            // prepare reply
            duration := time.Duration(storagerpc.LeaseSeconds) * time.Second
            reply.Lease = &storagerpc.Lease{Granted: true, Duration: duration}

            // record lease
            o := &owner{leaseEnd: time.Now().Add(duration)}
            o.HostPort = args.HostPort
            prot.leaseOwners.PushBack(o)
        }
        prot.bMLock.Unlock()
    } else {
        ss.listStoreLock.Unlock()
    }

    reply.Status = storagerpc.OK
    reply.Value = val
	return nil
}

// Put inserts the specified key/value pair into the data store. If
// the key does not fall within the storage server's range, it should
// reply with status WrongServer.
func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {

    // check that the key is in the server's hash range
    keyHash := libstore.StoreHash(strings.Split(args.Key,":")[0])
    if !inRange(keyHash, ss.minKey, ss.maxKey) {
        reply.Status = storagerpc.WrongServer
        return nil
    }

    // if key doesn't exist, just add it
    ss.simpleStoreLock.Lock()
    if _, pres := ss.simpleStore[args.Key]; !pres {
        ss.simpleStore[args.Key] = args.Value

        // make new protector
        prot := &protector{leaseOwners: list.New()}
        prot.storeLock = new(&sync.Mutex)
        prot.bMLock = new(&sync.Mutex)
        ss.simpleProtectors[args.Key] = prot

        ss.simpleStoreLock.Unlock()
        reply.Status = storagerpc.OK
        return nil
    }

    // switch to fine-grained locking
    prot := ss.simpleProtectors[args.Key]
    ss.simpleStoreLock.Unlock()
    prot.storeLock.Lock()

    // indicate modifying
    prot.bMLock.Lock()
    prot.beingModified = true
    prot.bMLock.Unlock()

    // revoke unexpired leases
    l := prot.leaseOwners
    for ; l.Len() > 0; {
        e := l.Front()

        guard := time.Duration(storagerpc.LeaseGuardSeconds) * time.Second
        // if unexpired
        if e.Value.(*owner).LeaseEnd.Add(guard).After(time.Now()) {

            // prepare RPC
            RLArgs := &RevokeLeaseArgs{args.Key}
            RLReply := new(RevokeLeaseReply)
            libRPC, err := rpc.DialHTTP("tcp", e.Value.(*owner).HostPort)
	        if err != nil {
                return err
            }

            err := masterRPC.Call("LeaseCallbacks.RevokeLease",RLArgs,RLReply)
	        if err != nil {
                return err
            }
        }
        l.Remove(e)
    }

    // modify/enter value
    ss.simpleStore[args.Key] = args.Value

    // no longer modifying
    prot.bMLock.Lock()
    prot.beingModified = false
    prot.bMLock.Unlock()

    prot.storeLock.Unlock()
    reply.Status = storagerpc.OK
	return nil
}

// AppendToList retrieves the specified key from the data store and appends
// the specified value to its list. If the key does not fall within the
// receiving server's range, it should reply with status WrongServer. If
// the specified value is already contained in the list, it should reply
// with status ItemExists.
//type PutArgs struct {
//	Key   string
//	Value string
//}
//
//type PutReply struct {
//	Status Status
//}
//
func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {

    // check that the key is in the server's hash range
    keyHash := libstore.StoreHash(strings.Split(args.Key,":")[0])
    if !inRange(keyHash, ss.minKey, ss.maxKey) {
        reply.Status = storagerpc.WrongServer
        return nil
    }

    // if key doesn't exist, just add it
    ss.listStoreLock.Lock()
    if _, pres := ss.listStore[args.Key]; !pres {

        ss.listStore[args.Key] := []string { args.Value }

        // make new protector
        prot := &protector{leaseOwners: list.New()}
        prot.storeLock = new(&sync.Mutex)
        prot.bMLock = new(&sync.Mutex)
        ss.listProtectors[args.Key] = prot

        ss.listStoreLock.Unlock()
        reply.Status = storagerpc.OK
        return nil
    }

    // switch to fine-grained locking
    prot := ss.listProtectors[args.Key]
    ss.listStoreLock.Unlock()
    prot.storeLock.Lock()

    // indicate modifying
    prot.bMLock.Lock()
    prot.beingModified = true
    prot.bMLock.Unlock()

    // revoke unexpired leases
    l := prot.leaseOwners
    for ; l.Len() > 0; {
        e := l.Front()

        guard := time.Duration(storagerpc.LeaseGuardSeconds) * time.Second
        // if unexpired
        if e.Value.(*owner).LeaseEnd.Add(guard).After(time.Now()) {

            // prepare RPC
            RLArgs := &RevokeLeaseArgs{args.Key}
            RLReply := new(RevokeLeaseReply)
            libRPC, err := rpc.DialHTTP("tcp", e.Value.(*owner).HostPort)
	        if err != nil {
                return err
            }

            err := masterRPC.Call("LeaseCallbacks.RevokeLease",RLArgs,RLReply)
	        if err != nil {
                return err
            }
        }
        l.Remove(e)
    }

    // check whether item already exists
    entryL := ss.listStore[args.Key]
    for i := 0; i < len(); i++ {
        if  entryL[i] == args.Value {

            // no longer modifying
            prot.bMLock.Lock()
            prot.beingModified = false
            prot.bMLock.Unlock()
            prot.storeLock.Unlock()

            reply.Status = ItemExists
            return nil
        }
    }

    // append value to appropriate list
    ss.listStore[args.Key] = append(ss.listStore[args.Key], args.Value)

    // no longer modifying
    prot.bMLock.Lock()
    prot.beingModified = false
    prot.bMLock.Unlock()

    prot.storeLock.Unlock()
    reply.Status = storagerpc.OK
	return nil
}

// RemoveFromList retrieves the specified key from the data store and removes
// the specified value from its list. If the key does not fall within the
// receiving server's range, it should reply with status WrongServer. If
// the specified value is not already contained in the list, it should reply
// with status ItemNotFound.
//type PutArgs struct {
//	Key   string
//	Value string
//}
//
//type PutReply struct {
//	Status Status
//}
//
func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {

    // check that the key is in the server's hash range
    keyHash := libstore.StoreHash(strings.Split(args.Key,":")[0])
    if !inRange(keyHash, ss.minKey, ss.maxKey) {
        reply.Status = storagerpc.WrongServer
        return nil
    }
	return errors.New("not implemented")
}
