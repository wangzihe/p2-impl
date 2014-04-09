package libstore

import (
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

// struct that stores lease information about a key stored in local cache
type LocalInfo struct {
	LocalLease storagerpc.Lease // lease for a key
	IssueTime  time.Time        // issue time (in unix nano seconds for the lease)
}

type libstore struct {
	mode             LeaseMode                // mode of requesting lease
	localSimpleStore map[string]string        // map that stores string to string mapping
	localListStore   map[string]([]string)    // map that stores string to slice of string mapping
	simpleRWMutex    *sync.RWMutex            // read/write lock to protect localSimpleStore
	listRWMutex      *sync.RWMutex            // read/write lock to protect localListStore
	simpleLocalInfo  map[string]*LocalInfo    // map that stores lease info of keys in localSimpleStore
	listLocalInfo    map[string]*LocalInfo    // map that stores lease info of keys in localListStore
	servers          []storagerpc.Node        // list of storage servers in the network
	connections      map[string](*rpc.Client) // map that stores connections to storage servers(key: hostport)
	connectionsLock  *sync.RWMutex            // read/write lock that protect connections map
	queryCounts      map[string]int           // map that stores query counts for each key
	queryCountsMutex *sync.Mutex              // lock to protext queryCounts map
	libstoreLOGV     *log.Logger              // logger for libstore
	myHostPort       string                   // callback hostport for the libstore
}

//Implement a sort interface for []storagerpc.Node
type serverSlice []storagerpc.Node

func (servers serverSlice) Len() int           { return len(servers) }
func (servers serverSlice) Swap(i, j int)      { servers[i], servers[j] = servers[j], servers[i] }
func (servers serverSlice) Less(i, j int) bool { return servers[i].NodeID < servers[j].NodeID }

// This function contacts a storage server and saves the connection
// if it is not in the list of saved connections.
func contactServer(hostPort string,
	connections map[string](*rpc.Client), connectionsLock *sync.RWMutex,
	log *log.Logger) (*rpc.Client, error) {
	// check whether the connection is already saved
	var cli *rpc.Client
	var exist bool

	connectionsLock.RLock()
	cli, exist = connections[hostPort]
	connectionsLock.RUnlock()
	if exist == false {
		// the connection is not saved, create one and save it
		var err error
		cli, err = rpc.DialHTTP("tcp", hostPort)
		if err != nil {
			log.Printf("contactServer: error occurred while dialing. %s\n", err)
			return nil, err
		} else {
			// save the connection
			connectionsLock.Lock()
			_, exist = connections[hostPort]
			// check again so that no one else insert a connection for
			// this server after we release read lock
			if exist == false {
				connections[hostPort] = cli
			}
			connectionsLock.Unlock()
		}
	}

	return cli, nil
}

//for debug
func (ls *libstore) printServers() {
	for index := 0; index < len(ls.servers); index++ {
		ls.libstoreLOGV.Printf("server: %s with id %d\n", ls.servers[index].HostPort, ls.servers[index].NodeID)
	}
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	// check parameter validity
	if masterServerHostPort == "" {
		fmt.Printf("invalid master server hostport\n")
		return nil, errors.New("invalid master server hostport")
	}

	// initialize data structures and locks
	lib := new(libstore)
	lib.localSimpleStore = make(map[string]string)
	lib.localListStore = make(map[string]([]string))
	lib.simpleRWMutex = &sync.RWMutex{}
	lib.listRWMutex = &sync.RWMutex{}
	lib.simpleLocalInfo = make(map[string]*LocalInfo)
	lib.listLocalInfo = make(map[string]*LocalInfo)
	lib.connections = make(map[string](*rpc.Client))
	lib.connectionsLock = &sync.RWMutex{}
	lib.queryCounts = make(map[string]int)
	lib.queryCountsMutex = &sync.Mutex{}
	lib.myHostPort = myHostPort
	lib.mode = mode

	// create logger
	var fileName string
	if myHostPort != "" {
		tempParts := strings.Split(myHostPort, ":")
		fileName = "./libstore:" + tempParts[0] + tempParts[1] + ".txt"
	} else {
		fileName = "./libstore.txt"
	}
	logfile, _ := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	lib.libstoreLOGV = log.New(logfile, "VERBOSE", log.Lmicroseconds|log.Lshortfile)

	lib.libstoreLOGV.Printf("libstore %s: start logging\n", myHostPort)

	// register reoveLease
	rpc.RegisterName("LeaseCallbacks", librpc.Wrap(lib))

	// contact master storage server to get list of servers
	lib.libstoreLOGV.Printf("start contacting master server\n")
	count := 0
	for count < 5 {
		cli, err := contactServer(masterServerHostPort, lib.connections,
			lib.connectionsLock, lib.libstoreLOGV)
		if err != nil {
			lib.libstoreLOGV.Printf("libstore %s: contactServer error.\n", err)
			return nil, err
		}
		args := &storagerpc.GetServersArgs{}
		var reply storagerpc.GetServersReply
		err = cli.Call("StorageServer.GetServers", args, &reply)
		if err != nil {
			lib.libstoreLOGV.Printf("libstore: GetServers rpc error. %s\n", err)
			return nil, err
		} else {
			if reply.Status == storagerpc.OK {
				lib.libstoreLOGV.Printf("get ok response from master storage server\n")
				lib.servers = reply.Servers
				sort.Sort(serverSlice(lib.servers))
				lib.printServers()

				// create timer that reset query counts every QueryCacheSeconds
				go func() {
					for {
						time.Sleep(storagerpc.QueryCacheSeconds * time.Second)
						lib.queryCountsMutex.Lock()
						for k, _ := range lib.queryCounts {
							lib.queryCounts[k] = 0
						}
						lib.queryCountsMutex.Unlock()
					}
				}()

				// create lease cleaner that cleans expired lease
				go func() {
					for {
						time.Sleep(1 * time.Second)
						newSimpleLocalInfo := make(map[string]*LocalInfo)
						newListLocalInfo := make(map[string]*LocalInfo)
						lib.simpleRWMutex.Lock()
						for k, _ := range lib.simpleLocalInfo {
							temp := lib.simpleLocalInfo[k]
							if checkExpiration(temp) == false {
								// lease didn't expire
								newSimpleLocalInfo[k] = temp
							} else {
								// lease expires
								delete(lib.localSimpleStore, k)
							}
							lib.simpleLocalInfo[k] = nil
						}
						lib.simpleLocalInfo = newSimpleLocalInfo
						lib.simpleRWMutex.Unlock()
						lib.listRWMutex.Lock()
						for k, _ := range lib.listLocalInfo {
							temp := lib.listLocalInfo[k]
							if checkExpiration(temp) == false {
								// lease hasn't expired
								newListLocalInfo[k] = temp
							} else {
								// lease expires
								delete(lib.localListStore, k)
							}
							lib.listLocalInfo[k] = nil
						}
						lib.listLocalInfo = newListLocalInfo
						lib.listRWMutex.Unlock()
					}
				}()
				return lib, nil
			} else {
				lib.libstoreLOGV.Printf("get NotReady response from master storage server\n")
				time.Sleep(1 * time.Second)
			}
		}
		count = count + 1
	}

	lib.libstoreLOGV.Printf("timed out when starting libstore\n")
	return nil, errors.New("timed out when starting libstore")
}

// This function checks whether a lease expires or not.
// It returns true if the lease expires. False otherwise.
func checkExpiration(localCopy *LocalInfo) bool {
	if localCopy != nil {
		lease := localCopy.LocalLease
		issueTime := localCopy.IssueTime
		guard := time.Duration(lease.ValidSeconds) * time.Second
		leaseEnd := issueTime.Add(guard)
		if leaseEnd.After(time.Now()) == false {
			// lease expired
			return true
		} else {
			return false
		}
	}
	return true
}

// This function selects the appropriate storage server based
// on the input key and returns the connection to that server.
func (ls *libstore) selectServer(key string) *rpc.Client {
	keyHash := StoreHash(key)
	ls.libstoreLOGV.Printf("selectServer: key is %s, keyHash is %d\n", key, keyHash)
	var server storagerpc.Node = storagerpc.Node{"none", 0}

	for index := 0; index < len(ls.servers)-1; index++ {
		current := ls.servers[index]
		nextNode := ls.servers[index+1]
		ls.libstoreLOGV.Printf("selectServer: current Node ID is %d, next Node ID is %d\n", current.NodeID, nextNode.NodeID)
		if current.NodeID == keyHash {
			server = current
			break
		}
		if current.NodeID < keyHash && keyHash < nextNode.NodeID {
			//fmt.Printf("here\n")
			server = nextNode
		}
	}

	if server.HostPort == "none" {
		length := len(ls.servers)
		if keyHash == ls.servers[length-1].NodeID {
			server = ls.servers[length-1]
		} else {
			server = ls.servers[0]
		}
	}

	ls.libstoreLOGV.Printf("selectServer: server selected is %s with id %d\n", server.HostPort, server.NodeID)
	var cli *rpc.Client
	var exist bool

	ls.connectionsLock.RLock()
	cli, exist = ls.connections[server.HostPort]
	ls.connectionsLock.RUnlock()
	if exist == false {
		ls.libstoreLOGV.Printf("selectServer: server connection isn't saved\n")
		// the connection is not saved, create one and save it
		var err error
		ls.libstoreLOGV.Printf("selectServer: about to contact server %s\n", server.HostPort)
		dialKey := strings.Split(server.HostPort, ":")[1] + ":" + strings.Split(server.HostPort, ":")[2]
		cli, err = rpc.DialHTTP("tcp", dialKey)
		if err != nil {
			ls.libstoreLOGV.Printf("selectServer: error while dialing: %s\n", err)
			return nil
		} else {
			// save the connection
			ls.connectionsLock.Lock()
			_, exist = ls.connections[server.HostPort]
			// check again so that no one else insert a connection for
			// this server after we release read lock
			if exist == false {
				ls.connections[server.HostPort] = cli
			}
			ls.connectionsLock.Unlock()
		}
	}

	return cli
}

// This function calls the Get RPC function of storage server.
func (ls *libstore) callGetRPC(key string, wantLease bool,
	callBackPort string, reply *storagerpc.GetReply) error {
	cli := ls.selectServer(strings.Split(key, ":")[0])
	if cli == nil {
		ls.libstoreLOGV.Printf("callGetRPC: server returned is nil\n")
		return errors.New("no server selected")
	} else {
		args := &storagerpc.GetArgs{key, wantLease, callBackPort}
		err := cli.Call("StorageServer.Get", args, reply)
		return err
	}
}

// This function handles the reply of the Get RPC function of
// storage server.
// create parameter is a flag that indicates whether the
// key exists in localSimpleStore. create=1 means the key
// doesn't exist and we need to create new entries in
// both localSimpleStore and simpleLocalInfo map.
func (ls *libstore) handleGetRPC(reply storagerpc.GetReply, key string,
	wantLease bool, create int) (string, error) {
	if reply.Status == storagerpc.OK {
		if wantLease == true && reply.Lease.Granted == true {
			// lease is granted by server. Update lease and local copy
			ls.updateSimpleValueAndLease(key, reply, create)
		}
		// return value
		return reply.Value, nil
	} else {
		if reply.Status == storagerpc.KeyNotFound {
			return "", errors.New("key not found")
		} else if reply.Status == storagerpc.WrongServer {
			return "", errors.New("wrong server")
		} else {
			return "", errors.New("unexpected status")
		}
	}
}

// This function calls the GetList RPC function of storage server.
func (ls *libstore) callGetListRPC(key string, wantLease bool,
	callBackPort string, reply *storagerpc.GetListReply) error {
	cli := ls.selectServer(strings.Split(key, ":")[0])
	if cli == nil {
		ls.libstoreLOGV.Printf("callGetListRPC: server returned is nil\n")
		return errors.New("no server selected")
	} else {
		args := &storagerpc.GetArgs{key, wantLease, callBackPort}
		err := cli.Call("StorageServer.GetList", args, reply)
		return err
	}
}

// This function handles the reply of the GetList RPC function of
// storage server.
// create parameter is a flag that indicates whether the key exists
// in localListStore. create=1 means the key doesn't exist and we
// need to create new entries in both localListStore and
// listLocalInfo map.
func (ls *libstore) handleGetListRPC(reply storagerpc.GetListReply,
	key string, wantLease bool, create int) ([]string, error) {
	if reply.Status == storagerpc.OK {
		if wantLease == true && reply.Lease.Granted == true {
			// lease is granted by server. Update lease and local copy
			ls.updateListValueAndLease(key, reply, create)
		}
		// return value
		return reply.Value, nil
	} else {
		if reply.Status == storagerpc.KeyNotFound {
			return nil, errors.New("key not found")
		} else if reply.Status == storagerpc.WrongServer {
			return nil, errors.New("wrong server")
		} else {
			return nil, errors.New("unexpected status")
		}
	}
}

// This function updates value and lease for key in the
// localListStore.
// create parameter is a flag taht indicates whether the key
// exists in localListStore. create=1 means the key doesn't
// exist and we need to create new entries in both
// localListStore and listLocalInfo map.
func (ls *libstore) updateListValueAndLease(key string,
	reply storagerpc.GetListReply, create int) {
	if create == 0 {
		// key already exists, simply update
		ls.listRWMutex.Lock()
		newLocalInfo := ls.listLocalInfo[key]
		if newLocalInfo != nil {
			newLocalInfo.LocalLease = reply.Lease
			newLocalInfo.IssueTime = time.Now()
		} else {
			ls.listLocalInfo[key] = &LocalInfo{reply.Lease, time.Now()}
		}
		ls.localListStore[key] = reply.Value
		ls.listRWMutex.Unlock()
	} else {
		// key doesn't exist, create new entries
		ls.listRWMutex.Lock()
		newInfo := &LocalInfo{reply.Lease, time.Now()}
		ls.listLocalInfo[key] = newInfo
		ls.localListStore[key] = reply.Value
		ls.listRWMutex.Unlock()
	}
}

// This function updates value and lease for key in the
// localSimpleStore.
// create parameter is a flag that indicates whether the
// key exists in localSimpleStore. create=1 means the key
// doesn't exist and we need to create new entries in
// both localSimpleStore and simpleLocalInfo map.
func (ls *libstore) updateSimpleValueAndLease(key string,
	reply storagerpc.GetReply, create int) {
	if create == 0 {
		// key already exists, simply update
		ls.simpleRWMutex.Lock()
		//fmt.Printf("Get: lock write lock\n")
		newLocalInfo := ls.simpleLocalInfo[key]
		if newLocalInfo != nil {
			newLocalInfo.LocalLease = reply.Lease
			newLocalInfo.IssueTime = time.Now()
		} else {
			ls.simpleLocalInfo[key] = &LocalInfo{reply.Lease, time.Now()}
		}
		ls.localSimpleStore[key] = reply.Value
		ls.simpleRWMutex.Unlock()
		//fmt.Printf("Get: unlock write lock\n")
	} else {
		// key doesn't exist, create new entries
		ls.simpleRWMutex.Lock()
		//fmt.Printf("Get: lock write lock\n")
		newInfo := &LocalInfo{reply.Lease, time.Now()}
		ls.simpleLocalInfo[key] = newInfo
		ls.localSimpleStore[key] = reply.Value
		ls.simpleRWMutex.Unlock()
		//fmt.Printf("Get: unlock write lock\n")
	}
}

// This function sets the wantLease variable based
// on lease mode and query count.
func (ls *libstore) setWantLease(count int) bool {
	if ls.mode == Never {
		return false
	} else if ls.mode == Always {
		return true
	} else {
		if count >= storagerpc.QueryCacheThresh {
			return true
		} else {
			return false
		}
	}
}

// Right now, this function returns error when key is not
// found on storage server or when libstore contacts the
// wrong server.
func (ls *libstore) Get(key string) (string, error) {
	//fmt.Printf("enter get function\n")
	// update query count for the key
	ls.queryCountsMutex.Lock()
	_, exist := ls.queryCounts[key]
	if exist == false {
		ls.queryCounts[key] = 1
	} else {
		ls.queryCounts[key] += 1
	}
	count := ls.queryCounts[key]
	ls.queryCountsMutex.Unlock()

	// obtain value from local cache if possible.
	// otherwise, contact storage server
	ls.simpleRWMutex.RLock()
	//fmt.Printf("Get: lock read lock\n")
	localInfo, pres := ls.simpleLocalInfo[key]
	if pres == true {
		// key exists in local cache.
		if localInfo == nil || checkExpiration(localInfo) == true {
			// lease expires
			ls.simpleRWMutex.RUnlock()
			//fmt.Printf("Get: release read lock\n")
			wantLease := ls.setWantLease(count)
			// contact storage server
			var reply storagerpc.GetReply
			getErr := ls.callGetRPC(key, wantLease, ls.myHostPort, &reply)
			if getErr != nil {
				return "", getErr
			} else {
				// check reply
				val, err := ls.handleGetRPC(reply, key, wantLease, 0)
				//fmt.Printf("Get: exit 1\n")
				return val, err
			}
		} else {
			// lease is still valid
			val := ls.localSimpleStore[key]
			ls.simpleRWMutex.RUnlock()
			//fmt.Printf("Get: release read lock\n")
			//fmt.Printf("Get: exit 2\n")
			return val, nil
		}
	} else {
		// key does not exist
		ls.simpleRWMutex.RUnlock()
		//fmt.Printf("Get: release read lock\n")
		wantLease := ls.setWantLease(count)
		// call storage server Get RPC
		var reply storagerpc.GetReply
		getErr := ls.callGetRPC(key, wantLease, ls.myHostPort, &reply)
		if getErr != nil {
			return "", getErr
		} else {
			// check reply
			val, err := ls.handleGetRPC(reply, key, wantLease, 1)
			//fmt.Printf("Get: exit 3\n")
			return val, err
		}
	}
}

func (ls *libstore) Put(key, value string) error {
	cli := ls.selectServer(strings.Split(key, ":")[0])
	if cli == nil {
		ls.libstoreLOGV.Printf("Put: server returned is nil\n")
		return errors.New("no server selected")
	} else {
		args := &storagerpc.PutArgs{key, value}
		var reply storagerpc.PutReply
		err := cli.Call("StorageServer.Put", args, &reply)
		if err != nil {
			return err
		} else {
			if reply.Status != storagerpc.OK {
				if reply.Status == storagerpc.WrongServer {
					return errors.New("wrong server")
				} else {
					return errors.New("unexpected reply status")
				}
			} else {
				return nil
			}
		}
	}
}

func (ls *libstore) GetList(key string) ([]string, error) {
	// update query count for the key
	ls.queryCountsMutex.Lock()
	_, exist := ls.queryCounts[key]
	if exist == false {
		ls.queryCounts[key] = 1
	} else {
		ls.queryCounts[key] += 1
	}
	count := ls.queryCounts[key]
	ls.queryCountsMutex.Unlock()

	// obtain value from local cache if possible.
	// otherwise, contact storage server
	ls.listRWMutex.RLock()
	localInfo, pres := ls.listLocalInfo[key]
	if pres == true {
		// key exists in local cache.
		if localInfo == nil || checkExpiration(localInfo) == true {
			// lease expires
			ls.listRWMutex.RUnlock()
			wantLease := ls.setWantLease(count)
			// contact storage server
			var reply storagerpc.GetListReply
			getErr := ls.callGetListRPC(key, wantLease, ls.myHostPort, &reply)
			if getErr != nil {
				return nil, getErr
			} else {
				// check reply
				val, err := ls.handleGetListRPC(reply, key, wantLease, 0)
				return val, err
			}
		} else {
			// lease is still valid
			val := ls.localListStore[key]
			ls.listRWMutex.RUnlock()
			return val, nil
		}
	} else {
		// key does not exist
		ls.listRWMutex.RUnlock()
		wantLease := ls.setWantLease(count)
		// call storage server GetList RPC
		var reply storagerpc.GetListReply
		getErr := ls.callGetListRPC(key, wantLease, ls.myHostPort, &reply)
		if getErr != nil {
			return nil, getErr
		} else {
			// check reply
			val, err := ls.handleGetListRPC(reply, key, wantLease, 1)
			return val, err
		}
	}
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	cli := ls.selectServer(strings.Split(key, ":")[0])
	if cli == nil {
		ls.libstoreLOGV.Printf("RemoveFromList: server returned is nil\n")
		return errors.New("no server selected")
	} else {
		args := &storagerpc.PutArgs{key, removeItem}
		var reply storagerpc.PutReply
		err := cli.Call("StorageServer.RemoveFromList", args, &reply)
		if err != nil {
			return err
		} else {
			if reply.Status != storagerpc.OK {
				if reply.Status == storagerpc.WrongServer {
					return errors.New("wrong server")
				} else if reply.Status == storagerpc.ItemNotFound {
					return errors.New("item not found")
				} else {
					return errors.New("unexpected reply status")
				}
			} else {
				return nil
			}
		}
	}
}

func (ls *libstore) AppendToList(key, newItem string) error {
	cli := ls.selectServer(strings.Split(key, ":")[0])
	if cli == nil {
		ls.libstoreLOGV.Printf("AppendToList: server returned is nil\n")
		return errors.New("no server selected")
	} else {
		args := &storagerpc.PutArgs{key, newItem}
		var reply storagerpc.PutReply
		err := cli.Call("StorageServer.AppendToList", args, &reply)
		if err != nil {
			return err
		} else {
			if reply.Status != storagerpc.OK {
				if reply.Status == storagerpc.WrongServer {
					return errors.New("wrong server")
				} else if reply.Status == storagerpc.ItemExists {
					return errors.New("Item exists")
				} else {
					return errors.New("unexpected reply status")
				}
			} else {
				return nil
			}
		}
	}
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	key := args.Key
	ls.simpleRWMutex.RLock()
	_, exist := ls.localSimpleStore[key]
	if exist == true {
		// the key exists
		ls.simpleRWMutex.RUnlock()
		ls.simpleRWMutex.Lock()
		delete(ls.simpleLocalInfo, key)
		delete(ls.localSimpleStore, key)
		ls.simpleRWMutex.Unlock()
		reply.Status = storagerpc.OK
		return nil
	} else {
		ls.simpleRWMutex.RUnlock()
		ls.listRWMutex.RLock()
		_, pres := ls.localListStore[key]
		if pres == true {
			ls.listRWMutex.RUnlock()
			ls.listRWMutex.Lock()
			delete(ls.listLocalInfo, key)
			delete(ls.localListStore, key)
			ls.listRWMutex.Unlock()
			reply.Status = storagerpc.OK
			return nil
		} else {
			ls.listRWMutex.RUnlock()
			reply.Status = storagerpc.KeyNotFound
			return nil
		}
	}
}
