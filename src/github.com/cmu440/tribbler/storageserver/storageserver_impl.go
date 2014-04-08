package storageserver

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type storageServer struct {

	// variables used for registration
	registerMutex             *sync.Mutex
	numRegistered, toRegister int
	allRegisteredChan         chan bool

	// hash ring info
	nodes          []storagerpc.Node
	minKey, maxKey uint32

	// primary data structures
	simpleStore map[string]string
	listStore   map[string]([]string)

	// locks for primary data structures; also lock protector maps
	simpleLock, listLock *sync.Mutex

	// locks for each element of primary data structures
	simpleProtectors, listProtectors map[string]protector

	// cache libstore RPC connections for lease revoking
	libstores    map[string](*rpc.Client)
	libstoreLock *sync.RWMutex

	LOGV *log.Logger
}

type owner struct {
	leaseEnd time.Time
	HostPort string
}

type protector struct {
	leaseOwners       *list.List
	beingModified     bool //bM
	storeLock, bMLock *sync.Mutex
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
	fmt.Printf("new server created\n")

	// initialize the storageServer struct
	server := new(storageServer)
	server.registerMutex = new(sync.Mutex)
	server.toRegister = numNodes
	server.numRegistered = 1 // include the master server
	server.maxKey = nodeID
	server.allRegisteredChan = make(chan bool)
	node := &storagerpc.Node{HostPort: "localhost:" + strconv.Itoa(port), NodeID: nodeID}

	server.simpleLock = new(sync.Mutex)
	server.listLock = new(sync.Mutex)
	server.simpleStore = make(map[string]string)
	server.listStore = make(map[string]([]string))
	server.simpleProtectors = make(map[string]protector)
	server.listProtectors = make(map[string]protector)
	server.libstores = make(map[string](*rpc.Client))
	server.libstoreLock = new(sync.RWMutex)

	// logging stuff
	fileName := "./" + strconv.Itoa(port) + ".txt"
	if masterServerHostPort == "" {
		fileName = "./master.txt"
	}
	logfile, _ := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	server.LOGV = log.New(logfile, "VERBOSE", log.Lmicroseconds|log.Lshortfile)
	server.LOGV.Printf("started log, nodeID: %d\n", nodeID)

	// create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", net.JoinHostPort("localhost", strconv.Itoa(port)))
	if err != nil {
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
		server.nodes = make([]storagerpc.Node, numNodes)
		server.nodes[0] = *node // register master as first node
		if numNodes > 1 {
			<-server.allRegisteredChan // wait until all nodes are registered
		}
		server.minKey = prevNodeID(nodeID, server.nodes)
		server.LOGV.Printf("minKey: %d, maxKey: %d \n", server.minKey, server.maxKey)
		return server, nil

		// if slave, call RegisterServer every second until acknowledged
	} else {

		// prepare to call RegisterServer
		masterRPC, err := rpc.DialHTTP("tcp", masterServerHostPort)
		if err != nil {
			return nil, err
		}
		args := &storagerpc.RegisterArgs{ServerInfo: *node}
		reply := &storagerpc.RegisterReply{Status: storagerpc.NotReady}

		for {
			go func() {
				err := masterRPC.Call("StorageServer.RegisterServer", args, reply)
				server.LOGV.Printf("registered server %d ", server.maxKey)
				if err != nil {
					server.LOGV.Printf("without error\n")
				} else {
					server.LOGV.Printf("with error:\n", err.Error())
				}
			}()
			if reply.Status == storagerpc.OK {
				server.nodes = reply.Servers
				server.minKey = prevNodeID(nodeID, server.nodes)
				server.LOGV.Printf("minKey: %d, maxKey: %d \n", server.minKey, server.maxKey)
				masterRPC.Close()
				return server, nil
			}
			<-time.After(time.Duration(1000) * time.Millisecond)
		}
	}
}

// helper function which returns the ID of the node whose ID immediately
// precedes nodeID in the hashing ring
func prevNodeID(nodeID uint32, nodes []storagerpc.Node) uint32 {

	// check if node has smallest hash
	isMin := true
	for i := 0; i < len(nodes); i++ {
		if nodes[i].NodeID < nodeID {
			isMin = false
			break
		}
	}

	// find the preceding node in the hash ring
	minKey := uint32(0)
	if isMin { // get max key
		for i := 0; i < len(nodes); i++ {
			if nodes[i].NodeID > minKey {
				minKey = nodes[i].NodeID
			}
		}
	} else { // get max key BEFORE nodeID
		for i := 0; i < len(nodes); i++ {
			if nodes[i].NodeID > minKey && nodes[i].NodeID < nodeID {
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
		unRegistered = unRegistered &&
			args.ServerInfo.NodeID != ss.nodes[i].NodeID
	}

	// register the node if it's new
	if unRegistered {
		ss.nodes[ss.numRegistered] = args.ServerInfo
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
	return (hash > minKey && hash <= maxKey) ||
		(minKey >= maxKey && (hash > minKey || hash <= maxKey))
}

// Get retrieves the specified key from the data store and replies with
// the key's value and a lease if one was requested. If the key does not
// fall within the storage server's range, it should reply with status
// WrongServer. If the key is not found, it should reply with status
// KeyNotFound.
func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {

	ss.LOGV.Println("REACHED GET")

	// check that the key is in the server's hash range
	keyHash := libstore.StoreHash(strings.Split(args.Key, ":")[0])
	if !inRange(keyHash, ss.minKey, ss.maxKey) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// check that the key exists
	ss.simpleLock.Lock()
	if val, pres := ss.simpleStore[args.Key]; pres {

		// handle leasing, if requested
		if args.WantLease {

			prot := ss.simpleProtectors[args.Key]
			prot.bMLock.Lock()
			ss.simpleLock.Unlock()

			if prot.beingModified { // reject lease request
				reply.Lease = storagerpc.Lease{Granted: false}

			} else { // grant lease
				// prepare reply
				reply.Lease = storagerpc.Lease{Granted: true}
				reply.Lease.ValidSeconds = storagerpc.LeaseSeconds

				// record lease
				duration := time.Duration(storagerpc.LeaseSeconds) * time.Second
				o := &owner{leaseEnd: time.Now().Add(duration)}
				o.HostPort = args.HostPort
				prot.leaseOwners.PushBack(o)
			}
			prot.bMLock.Unlock()
		} else {
			ss.simpleLock.Unlock()
		}

		reply.Status = storagerpc.OK
		reply.Value = val
		ss.LOGV.Printf("Get: %s   Got: %s\n", args.Key, val)
		if args.WantLease {
			ss.LOGV.Printf("Lease Wanted; Granted: %b\n", reply.Lease.Granted)
		}
		return nil
	}

	ss.simpleLock.Unlock()
	reply.Status = storagerpc.KeyNotFound
	return nil
}

// GetList retrieves the specified key from the data store and replies with
// the key's list value and a lease if one was requested. If the key does not
// fall within the storage server's range, it should reply with status
// WrongServer. If the key is not found, it should reply with status
// KeyNotFound.
func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {

	// check that the key is in the server's hash range
	keyHash := libstore.StoreHash(strings.Split(args.Key, ":")[0])
	if !inRange(keyHash, ss.minKey, ss.maxKey) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// check that the key exists
	ss.listLock.Lock()
	if val, pres := ss.listStore[args.Key]; pres {

		// handle leasing, if requested
		if args.WantLease {

			prot := ss.listProtectors[args.Key]
			prot.bMLock.Lock()
			ss.listLock.Unlock()

			if prot.beingModified { // reject lease request
				reply.Lease = storagerpc.Lease{Granted: false}

			} else { // grant lease
				// prepare reply
				reply.Lease = storagerpc.Lease{Granted: true}
				reply.Lease.ValidSeconds = storagerpc.LeaseSeconds

				// record lease
				duration := time.Duration(storagerpc.LeaseSeconds) * time.Second
				o := &owner{leaseEnd: time.Now().Add(duration)}
				o.HostPort = args.HostPort
				prot.leaseOwners.PushBack(o)
			}
			prot.bMLock.Unlock()
		} else {
			ss.listLock.Unlock()
		}

		reply.Status = storagerpc.OK
		reply.Value = val
		ss.LOGV.Printf("GetList: %s   Got:  (length: %d)\n", args.Key, len(val))
		for i := 0; i < len(val); i++ {
			ss.LOGV.Printf("%s \n", val[i])
		}
		if args.WantLease {
			ss.LOGV.Printf("Lease Wanted; Granted: %b\n", reply.Lease.Granted)
		}
		return nil
	}

	ss.listLock.Unlock()
	reply.Status = storagerpc.KeyNotFound
	return nil
}

// Put inserts the specified key/value pair into the data store. If
// the key does not fall within the storage server's range, it should
// reply with status WrongServer.
func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {

	ss.LOGV.Printf("Put Key: %s   Value: %s", args.Key, args.Value)

	// check that the key is in the server's hash range
	keyHash := libstore.StoreHash(strings.Split(args.Key, ":")[0])
	if !inRange(keyHash, ss.minKey, ss.maxKey) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// if key doesn't exist, just add it
	ss.simpleLock.Lock()
	if _, pres := ss.simpleStore[args.Key]; !pres {
		ss.simpleStore[args.Key] = args.Value

		// make new protector
		prot := &protector{leaseOwners: list.New()}
		prot.storeLock = new(sync.Mutex)
		prot.bMLock = new(sync.Mutex)
		ss.simpleProtectors[args.Key] = *prot

		ss.simpleLock.Unlock()
		reply.Status = storagerpc.OK
		return nil
	}

	// switch to fine-grained locking
	prot := ss.simpleProtectors[args.Key]
	ss.simpleLock.Unlock()
	prot.storeLock.Lock()

	// indicate modifying
	prot.bMLock.Lock()
	prot.beingModified = true
	prot.bMLock.Unlock()

	// revoke unexpired leases
	l := prot.leaseOwners
	for l.Len() > 0 {
		e := l.Front()

		guard := time.Duration(storagerpc.LeaseGuardSeconds) * time.Second
		// if unexpired
		if e.Value.(*owner).leaseEnd.Add(guard).After(time.Now()) {

			// prepare RPC
			RLArgs := &storagerpc.RevokeLeaseArgs{Key: args.Key}
			var RLReply storagerpc.RevokeLeaseReply

			// get tcp connection appropriate libstore, from cache if possible
			ss.libstoreLock.RLock()
			ss.LOGV.Printf("lease callback is %s\n", e.Value.(*owner).HostPort)
			var libRPC *rpc.Client
			var pres bool
			libRPC, pres = ss.libstores[e.Value.(*owner).HostPort]
			ss.libstoreLock.RUnlock()
			if !pres { // add to cache if not present
				var err error
				libRPC, err = rpc.DialHTTP("tcp", e.Value.(*owner).HostPort)
				ss.libstoreLock.Lock()
				//TODO should do another check to make sure nobody added this connection before we make changes
				//     need to check whether the value still missing in the map
				ss.libstores[e.Value.(*owner).HostPort] = libRPC
				ss.libstoreLock.Unlock()
				if err != nil {
					return err
				}
			}

			ss.LOGV.Printf("about to revoke lease (key : %s)\n", args.Key)
			if libRPC == nil {
				ss.LOGV.Printf("shit\n")
			}
			err := libRPC.Call("LeaseCallbacks.RevokeLease", RLArgs, &RLReply)
			ss.LOGV.Printf("revoked lease (key: %s) ", args.Key)
			if err != nil {
				ss.LOGV.Printf("without error\n")
			} else {
				ss.LOGV.Printf("with error:\n", err.Error())
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

	ss.LOGV.Printf("AppendToList Key: %s   Value: %s", args.Key, args.Value)

	// check that the key is in the server's hash range
	keyHash := libstore.StoreHash(strings.Split(args.Key, ":")[0])
	if !inRange(keyHash, ss.minKey, ss.maxKey) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// if key doesn't exist, just add it
	ss.listLock.Lock()
	if _, pres := ss.listStore[args.Key]; !pres {

		ss.listStore[args.Key] = []string{args.Value}

		// make new protector
		prot := &protector{leaseOwners: list.New()}
		prot.storeLock = new(sync.Mutex)
		prot.bMLock = new(sync.Mutex)
		ss.listProtectors[args.Key] = *prot

		ss.listLock.Unlock()
		reply.Status = storagerpc.OK
		return nil
	}

	// switch to fine-grained locking
	prot := ss.listProtectors[args.Key]
	ss.listLock.Unlock()
	prot.storeLock.Lock()

	// indicate modifying
	prot.bMLock.Lock()
	prot.beingModified = true
	prot.bMLock.Unlock()

	// revoke unexpired leases
	l := prot.leaseOwners
	for l.Len() > 0 {
		e := l.Front()

		guard := time.Duration(storagerpc.LeaseGuardSeconds) * time.Second
		// if unexpired
		if e.Value.(*owner).leaseEnd.Add(guard).After(time.Now()) {

			// prepare RPC
			RLArgs := &storagerpc.RevokeLeaseArgs{args.Key}
			RLReply := new(storagerpc.RevokeLeaseReply)

			// get tcp connection appropriate libstore, from cache if possible
			ss.libstoreLock.RLock()
			libRPC, pres := ss.libstores[e.Value.(*owner).HostPort]
			ss.libstoreLock.RUnlock()
			if !pres { // add to cache if not present
				libRPC, err := rpc.DialHTTP("tcp", e.Value.(*owner).HostPort)
				ss.libstoreLock.Lock()
				ss.libstores[e.Value.(*owner).HostPort] = libRPC
				ss.libstoreLock.Unlock()
				if err != nil {
					return err
				}
			}

			go func() {
				libRPC.Call("LeaseCallbacks.RevokeLease", RLArgs, RLReply)
			}()
		}
		l.Remove(e)
	}

	// check whether item already exists
	for i := 0; i < len(ss.listStore[args.Key]); i++ {
		if ss.listStore[args.Key][i] == args.Value {

			// no longer modifying
			prot.bMLock.Lock()
			prot.beingModified = false
			prot.bMLock.Unlock()
			prot.storeLock.Unlock()

			reply.Status = storagerpc.ItemExists
			ss.LOGV.Println("AppendToList ItemExists")
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

	ss.LOGV.Printf("RemoveFromList Key: %s   Value: %s", args.Key, args.Value)

	// check that the key is in the server's hash range
	keyHash := libstore.StoreHash(strings.Split(args.Key, ":")[0])
	if !inRange(keyHash, ss.minKey, ss.maxKey) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// if key doesn't exist, return status ItemNotFound
	ss.listLock.Lock()
	if _, pres := ss.listStore[args.Key]; !pres {
		ss.listLock.Unlock()
		reply.Status = storagerpc.ItemNotFound
		return nil
	}

	// switch to fine-grained locking
	prot := ss.listProtectors[args.Key]
	ss.listLock.Unlock()
	prot.storeLock.Lock()

	// indicate modifying
	prot.bMLock.Lock()
	prot.beingModified = true
	prot.bMLock.Unlock()

	// revoke unexpired leases
	l := prot.leaseOwners
	for l.Len() > 0 {
		e := l.Front()

		guard := time.Duration(storagerpc.LeaseGuardSeconds) * time.Second
		// if unexpired
		if e.Value.(*owner).leaseEnd.Add(guard).After(time.Now()) {

			// prepare RPC
			RLArgs := &storagerpc.RevokeLeaseArgs{args.Key}
			RLReply := new(storagerpc.RevokeLeaseReply)

			// get tcp connection appropriate libstore, from cache if possible
			ss.libstoreLock.RLock()
			libRPC, pres := ss.libstores[e.Value.(*owner).HostPort]
			ss.libstoreLock.RUnlock()
			if !pres { // add to cache if not present
				libRPC, err := rpc.DialHTTP("tcp", e.Value.(*owner).HostPort)
				ss.libstoreLock.Lock()
				ss.libstores[e.Value.(*owner).HostPort] = libRPC
				ss.libstoreLock.Unlock()
				if err != nil {
					return err
				}
			}

			go func() {
				libRPC.Call("LeaseCallbacks.RevokeLease", RLArgs, RLReply)
			}()
		}
		l.Remove(e)
	}

	found := false
	// find item in list
	for i := 0; i < len(ss.listStore[args.Key]); i++ {
		if ss.listStore[args.Key][i] == args.Value {
			// remove i^th item
			ss.listStore[args.Key] = append(ss.listStore[args.Key][:i], ss.listStore[args.Key][i+1:]...)
			found = true
			break
		}
	}

	// no longer modifying
	prot.bMLock.Lock()
	prot.beingModified = false
	prot.bMLock.Unlock()

	prot.storeLock.Unlock()

	if found {
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.ItemNotFound
		ss.LOGV.Println("RemoveFromList ItemNotFound")
	}
	return nil
}
