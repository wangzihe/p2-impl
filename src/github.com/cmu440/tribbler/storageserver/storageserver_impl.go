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
	ready                     bool // indicator for whether the ring is ready. True means ready

	// hash ring info
	nodes          []storagerpc.Node
	minKey, maxKey uint32

	// primary data structures
	simpleStore map[string]string
	listStore   map[string]([]string)

	// locks for primary data structures; also lock protector maps
	simpleLock, listLock *sync.Mutex

	// locks for each element of primary data structures
	simpleProtectors, listProtectors map[string](*protector)

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

	// initialize the storageServer struct
	server := new(storageServer)
	server.registerMutex = new(sync.Mutex)
	server.toRegister = numNodes
	server.numRegistered = 1 // include the master server
	server.maxKey = nodeID
	server.allRegisteredChan = make(chan bool)
	server.ready = false
	node := &storagerpc.Node{HostPort: "localhost:" + strconv.Itoa(port), NodeID: nodeID}

	server.simpleLock = new(sync.Mutex)
	server.listLock = new(sync.Mutex)
	server.simpleStore = make(map[string]string)
	server.listStore = make(map[string]([]string))
	server.simpleProtectors = make(map[string](*protector))
	server.listProtectors = make(map[string](*protector))
	server.libstores = make(map[string](*rpc.Client))
	server.libstoreLock = new(sync.RWMutex)
	fmt.Printf("new storage server\n")

	// logging stuff
	fileName := "./localhost" + strconv.Itoa(port) + ".txt"
	if masterServerHostPort == "" {
		fileName = "./master.txt"
	}
	logfile, _ := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	server.LOGV = log.New(logfile, "VERBOSE", log.Lmicroseconds|log.Lshortfile)
	server.LOGV.Printf("Starting storageServer: %s\n", net.JoinHostPort("localhost", strconv.Itoa(port)))
	server.LOGV.Printf("with hash: %d\n", nodeID)

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
	server.LOGV.Printf("about to set up the ring. number of nodes is %d\n", numNodes)
	if masterServerHostPort == "" {
		server.nodes = make([]storagerpc.Node, numNodes)
		server.nodes[0] = *node // register master as first node
		if numNodes > 1 {
			server.LOGV.Printf("waiting for all nodes to be registered\n")
			<-server.allRegisteredChan // wait until all nodes are registered
		}
		server.minKey = prevNodeID(nodeID, server.nodes)
		server.LOGV.Printf("finished setting the ring.\n")
		return server, nil

		// if slave, call RegisterServer every second until acknowledged
	} else {

		// prepare to call RegisterServer
		var masterRPC *rpc.Client
		for {
			masterRPC, err = rpc.DialHTTP("tcp", masterServerHostPort)
			if err == nil {
				break
			}
			<-time.After(time.Second)
		}

		for {
			args := &storagerpc.RegisterArgs{ServerInfo: *node}
			reply := &storagerpc.RegisterReply{Status: storagerpc.NotReady}
			err := masterRPC.Call("StorageServer.RegisterServer", args, reply)

			if err == nil && reply.Status == storagerpc.OK {
				server.nodes = reply.Servers
				server.LOGV.Printf("All %d hashes are:\n", len(server.nodes))
				for i := 0; i < len(server.nodes); i++ {
					server.LOGV.Printf("%d: ", i)
					server.LOGV.Printf("%d\n", server.nodes[i].NodeID)
				}
				server.LOGV.Printf("Done printing hashes\n")
				server.minKey = prevNodeID(nodeID, server.nodes)
				masterRPC.Close()
				return server, nil
			}

			<-time.After(time.Second)
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

	ss.LOGV.Printf("register server called\n")
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
		reply.Servers = ss.nodes
		if ss.ready == false {
			// should only pass signal to master server once. Otherwise, it might block
			ss.allRegisteredChan <- true
			ss.ready = true
		}
	} else {
		reply.Status = storagerpc.NotReady
	}
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	ss.LOGV.Printf("GetServers called\n")
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

	//ss.LOGV.Printf("Called Get Key: %s\n", args.Key)

	// check that the key is in the server's hash range
	keyHash := libstore.StoreHash(strings.Split(args.Key, ":")[0])
	if !inRange(keyHash, ss.minKey, ss.maxKey) {
		ss.LOGV.Printf("WrongServer, with Key: %s\n and hash: %d", args.Key, keyHash)
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

		//ss.LOGV.Printf("Got: %s\n", val)
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

	//ss.LOGV.Printf("Called GetList Key: %s\n", args.Key)

	// check that the key is in the server's hash range
	keyHash := libstore.StoreHash(strings.Split(args.Key, ":")[0])
	if !inRange(keyHash, ss.minKey, ss.maxKey) {
		ss.LOGV.Printf("WrongServer, with Key: %s\n and hash: %d", args.Key, keyHash)
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

	//ss.LOGV.Printf("Called Put Key: %s Value: %s\n", args.Key, args.Value)

	// check that the key is in the server's hash range
	keyHash := libstore.StoreHash(strings.Split(args.Key, ":")[0])
	if !inRange(keyHash, ss.minKey, ss.maxKey) {
		ss.LOGV.Printf("WrongServer, with Key: %s\n and hash: %d", args.Key, keyHash)
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// if key doesn't exist, just add it
	ss.simpleLock.Lock()
	if _, pres := ss.simpleStore[args.Key]; !pres {
		ss.simpleStore[args.Key] = args.Value

		// make new protector
		prot := &protector{leaseOwners: list.New()}
		prot.beingModified = false
		prot.storeLock = new(sync.Mutex)
		prot.bMLock = new(sync.Mutex)
		ss.simpleProtectors[args.Key] = prot

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
	lastExpiry := time.Now()
	numToRevoke := 0
	revokedChan := make(chan int, l.Len())
	for l.Len() > 0 {
		e := l.Front()

		guard := time.Duration(storagerpc.LeaseGuardSeconds) * time.Second

		if e.Value.(*owner).leaseEnd.Add(guard).After(lastExpiry) {
			lastExpiry = e.Value.(*owner).leaseEnd.Add(guard)
		}

		// if unexpired
		if e.Value.(*owner).leaseEnd.Add(guard).After(time.Now()) {

			// prepare RPC
			RLArgs := &storagerpc.RevokeLeaseArgs{Key: args.Key}
			var RLReply storagerpc.RevokeLeaseReply

			// get tcp connection appropriate libstore, from cache if possible
			ss.libstoreLock.RLock()
			var libRPC *rpc.Client
			var pres bool
			libRPC, pres = ss.libstores[e.Value.(*owner).HostPort]
			ss.libstoreLock.RUnlock()
			if !pres { // add to cache if not present

				ss.libstoreLock.Lock()
				// check that connection wasn't added while we switched locks
				libRPC, pres = ss.libstores[e.Value.(*owner).HostPort]
				if !pres { // add to cache if not present
					var err error
					libRPC, err = rpc.DialHTTP("tcp", e.Value.(*owner).HostPort)
					ss.libstores[e.Value.(*owner).HostPort] = libRPC
					if err != nil {
						return err
					}
				}
				ss.libstoreLock.Unlock()
			}

			numToRevoke++

			go func() { // goroutine calls RevokeLease
				libRPC.Call("LeaseCallbacks.RevokeLease", RLArgs, &RLReply)
				revokedChan <- 1
			}()
		}
		l.Remove(e)
	}

	doneChan := make(chan int, 10)

	go func() { // goroutine counts down revokedLeases
		for numToRevoke > 0 {
			select {
			case <-revokedChan:
				numToRevoke--
			case <-doneChan:
				return
			}
		}
		doneChan <- 1
		doneChan <- 1
	}()

	go func() { // goroutine waits for last lease to expire
		waitTime := lastExpiry.Sub(time.Now())
		if int64(waitTime) > 0 {
			select {
			case <-time.After(waitTime):
			case <-doneChan:
				return
			}
		}
		doneChan <- 1
		doneChan <- 1
	}()
	<-doneChan

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
func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {

	//ss.LOGV.Printf("Called AppendToList Key: %s Value: %s\n", args.Key, args.Value)

	// check that the key is in the server's hash range
	keyHash := libstore.StoreHash(strings.Split(args.Key, ":")[0])
	if !inRange(keyHash, ss.minKey, ss.maxKey) {
		ss.LOGV.Printf("WrongServer, with Key: %s\n and hash: %d", args.Key, keyHash)
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
		ss.listProtectors[args.Key] = prot

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
	lastExpiry := time.Now()
	numToRevoke := 0
	revokedChan := make(chan int, l.Len())
	for l.Len() > 0 {
		e := l.Front()

		guard := time.Duration(storagerpc.LeaseGuardSeconds) * time.Second

		if e.Value.(*owner).leaseEnd.Add(guard).After(lastExpiry) {
			lastExpiry = e.Value.(*owner).leaseEnd.Add(guard)
		}

		// if unexpired
		if e.Value.(*owner).leaseEnd.Add(guard).After(time.Now()) {

			// prepare RPC
			RLArgs := &storagerpc.RevokeLeaseArgs{Key: args.Key}
			var RLReply storagerpc.RevokeLeaseReply

			// get tcp connection appropriate libstore, from cache if possible
			ss.libstoreLock.RLock()
			var libRPC *rpc.Client
			var pres bool
			libRPC, pres = ss.libstores[e.Value.(*owner).HostPort]
			ss.libstoreLock.RUnlock()
			if !pres { // add to cache if not present

				ss.libstoreLock.Lock()
				// check that connection wasn't added while we switched locks
				libRPC, pres = ss.libstores[e.Value.(*owner).HostPort]
				if !pres { // add to cache if not present
					var err error
					libRPC, err = rpc.DialHTTP("tcp", e.Value.(*owner).HostPort)
					ss.libstores[e.Value.(*owner).HostPort] = libRPC
					if err != nil {
						return err
					}
				}
				ss.libstoreLock.Unlock()
			}

			numToRevoke++

			go func() { // goroutine calls RevokeLease
				libRPC.Call("LeaseCallbacks.RevokeLease", RLArgs, &RLReply)
				revokedChan <- 1
			}()
		}
		l.Remove(e)
	}

	doneChan := make(chan int, 10)

	go func() { // goroutine counts down revokedLeases
		for numToRevoke > 0 {
			select {
			case <-revokedChan:
				numToRevoke--
			case <-doneChan:
				return
			}
		}
		doneChan <- 1
		doneChan <- 1
	}()

	go func() { // goroutine waits for last lease to expire
		waitTime := lastExpiry.Sub(time.Now())
		if int64(waitTime) > 0 {
			select {
			case <-time.After(waitTime):
			case <-doneChan:
				return
			}
		}
		doneChan <- 1
		doneChan <- 1
	}()
	<-doneChan

	// check whether item already exists
	for i := 0; i < len(ss.listStore[args.Key]); i++ {
		if ss.listStore[args.Key][i] == args.Value {

			// no longer modifying
			prot.bMLock.Lock()
			prot.beingModified = false
			prot.bMLock.Unlock()
			prot.storeLock.Unlock()

			reply.Status = storagerpc.ItemExists
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
func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {

	//ss.LOGV.Printf("Called RemoveFromList Key: %s Value: %s\n", args.Key, args.Value)

	// check that the key is in the server's hash range
	keyHash := libstore.StoreHash(strings.Split(args.Key, ":")[0])
	if !inRange(keyHash, ss.minKey, ss.maxKey) {
		ss.LOGV.Printf("WrongServer, with Key: %s\n and hash: %d", args.Key, keyHash)
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
	lastExpiry := time.Now()
	numToRevoke := 0
	revokedChan := make(chan int, l.Len())
	for l.Len() > 0 {
		e := l.Front()

		guard := time.Duration(storagerpc.LeaseGuardSeconds) * time.Second

		if e.Value.(*owner).leaseEnd.Add(guard).After(lastExpiry) {
			lastExpiry = e.Value.(*owner).leaseEnd.Add(guard)
		}

		// if unexpired
		if e.Value.(*owner).leaseEnd.Add(guard).After(time.Now()) {

			// prepare RPC
			RLArgs := &storagerpc.RevokeLeaseArgs{Key: args.Key}
			var RLReply storagerpc.RevokeLeaseReply

			// get tcp connection appropriate libstore, from cache if possible
			ss.libstoreLock.RLock()
			var libRPC *rpc.Client
			var pres bool
			libRPC, pres = ss.libstores[e.Value.(*owner).HostPort]
			ss.libstoreLock.RUnlock()
			if !pres { // add to cache if not present

				ss.libstoreLock.Lock()
				// check that connection wasn't added while we switched locks
				libRPC, pres = ss.libstores[e.Value.(*owner).HostPort]
				if !pres { // add to cache if not present
					var err error
					libRPC, err = rpc.DialHTTP("tcp", e.Value.(*owner).HostPort)
					ss.libstores[e.Value.(*owner).HostPort] = libRPC
					if err != nil {
						return err
					}
				}
				ss.libstoreLock.Unlock()
			}

			numToRevoke++

			go func() { // goroutine calls RevokeLease
				libRPC.Call("LeaseCallbacks.RevokeLease", RLArgs, &RLReply)
				revokedChan <- 1
			}()
		}
		l.Remove(e)
	}

	doneChan := make(chan int, 10)

	go func() { // goroutine counts down revokedLeases
		for numToRevoke > 0 {
			select {
			case <-revokedChan:
				numToRevoke--
			case <-doneChan:
				return
			}
		}
		doneChan <- 1
		doneChan <- 1
	}()

	go func() { // goroutine waits for last lease to expire
		waitTime := lastExpiry.Sub(time.Now())
		if int64(waitTime) > 0 {
			select {
			case <-time.After(waitTime):
			case <-doneChan:
				return
			}
		}
		doneChan <- 1
		doneChan <- 1
	}()
	<-doneChan

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
	}
	return nil
}
