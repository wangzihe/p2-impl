package tribserver

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	//"os"
	"sort"
	"strconv"
	"time"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
)

type tribServer struct {
	Lib  libstore.Libstore //Libstore of the tribserver
	LOGV *log.Logger
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {

	server := new(tribServer)
	lib, err := libstore.NewLibstore(masterServerHostPort, myHostPort,
		libstore.Never)
	if err != nil {
		fmt.Printf("error while creating libstore\n")
		return nil, err
	}
	server.Lib = lib

	// logging stuff
	//fileName := "TribServer" + myHostPort + ".txt"
	//logfile, _ := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	server.LOGV = log.New(ioutil.Discard, "VERBOSE", log.Lmicroseconds|log.Lshortfile)
	server.LOGV.Printf("Starting tribServer: %s\n", myHostPort)

	// create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		fmt.Printf("error while creating server socket for incoming RPCs\n")
		return nil, err
	}

	// Wrap the tribServer before registering it for RPC.
	err = rpc.RegisterName("TribServer", tribrpc.Wrap(server))
	if err != nil {
		return nil, err
	}

	// Set up the HTTP handler that will serve incoming RPCs and
	// server requests in a background goroutine
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	return server, nil
}

// CreateUser creates a user with the specified UserID.
// Replies with status Exists if the user has previously been created.
func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {

	ts.LOGV.Printf("Called CreateUser on tribServer\n")

	key := args.UserID + ":create"

	if _, err := ts.Lib.Get(key); err != nil { // key does not already exist
		err = ts.Lib.Put(key, args.UserID) // add the key
		if err != nil {
			ts.LOGV.Printf("Lib.Get(key) returned error: %s\n", err.Error())
			return err
		}
		reply.Status = tribrpc.OK
	} else {
		reply.Status = tribrpc.Exists
	}
	return nil
}

// AddSubscription adds TargerUserID to UserID's list of subscriptions.
// Replies with status NoSuchUser if the specified UserID does not exist, and
// NoSuchTargerUser if the specified TargerUserID does not exist.
func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {

	// check that user and targetUser exist
	if _, err := ts.Lib.Get(args.UserID + ":create"); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	if _, err := ts.Lib.Get(args.TargetUserID + ":create"); err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	// add TargetUser to User subscription list
	err := ts.Lib.AppendToList(args.UserID+":sub", args.TargetUserID)
	if err != nil {
		reply.Status = tribrpc.Exists
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

// RemoveSubscription removes TargerUserID to UserID's list of subscriptions.
// Replies with status NoSuchUser if the specified UserID does not exist, and
// NoSuchTargerUser if the specified TargerUserID does not exist.
func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {

	// check that user and targetUser exist
	if _, err := ts.Lib.Get(args.UserID + ":create"); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	if _, err := ts.Lib.Get(args.TargetUserID + ":create"); err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	// remove TargetUser from User's subscription list
	err := ts.Lib.RemoveFromList(args.UserID+":sub", args.TargetUserID)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

// GetSubscriptions retrieves a list of all users to whom the user subscribes.
// Replies with status NoSuchUser if the specified UserID does not exist.
func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {

	// check that user exists
	if _, err := ts.Lib.Get(args.UserID + ":create"); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// get User's subscription list
	subs, err := ts.Lib.GetList(args.UserID + ":sub")
	if err != nil {
		reply.Status = tribrpc.OK
		reply.UserIDs = make([]string, 0)
		return nil
	}
	reply.Status = tribrpc.OK
	reply.UserIDs = subs
	return nil
}

// PostTribble posts a tribble on behalf of the specified UserID. The
// TribServer should timestamp the entry before inserting the Tribble into it's
// local Libstore.
func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {

	// check that user exists
	if _, err := ts.Lib.Get(args.UserID + ":create"); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	tribble := &tribrpc.Tribble{UserID: args.UserID, Posted: time.Now(), Contents: args.Contents}

	marshalled, err := json.Marshal(tribble)
	if err != nil {
		return err
	}
	// use timestamp as tribbleID
	tribbleID := strconv.FormatInt(tribble.Posted.UnixNano(), 10)

	// put (tribbleID, marshalled tribble) into the storageServer
	err = ts.Lib.Put(tribbleID, string(marshalled))
	if err != nil {
		return err
	}
	// append tribbleID to the User's tribble list
	err = ts.Lib.AppendToList(args.UserID+":tribble", tribbleID)
	if err != nil {
		return err
	}

	reply.Status = tribrpc.OK
	return nil
}

// GetTribbles retrieves a list of at most 100 tribbles posted by the specified
// UserID in reverse chronological order (most recent first).
// Replies with status NoSuchUser if the specified UserID does not exist.
func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {

	// check that user exists
	if _, err := ts.Lib.Get(args.UserID + ":create"); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// get list of UserID's tribbleIDs
	tribbleIDs, err := ts.Lib.GetList(args.UserID + ":tribble")
	if err != nil {
		// no tribbles posted by the user
		reply.Tribbles = make([]tribrpc.Tribble, 0)
		reply.Status = tribrpc.OK
		return nil
	}

	// sort the user's tribbleIDs in reverse chronological order
	sort.Sort(sort.Reverse(sort.StringSlice(tribbleIDs)))

	// allocate space for Tribbles
	numTribs := 100
	if len(tribbleIDs) < 100 {
		numTribs = len(tribbleIDs)
	}
	tribbles := make([]tribrpc.Tribble, numTribs)

	// get latest Tribbles from storageServer and unmarshall them
	for i := 0; i < numTribs; i++ {
		unmarshalled, err := ts.Lib.Get(tribbleIDs[i])
		if err != nil {
			return err
		}
		err = json.Unmarshal([]byte(unmarshalled), &(tribbles[i]))
		if err != nil {
			return err
		}
	}

	reply.Tribbles = tribbles
	reply.Status = tribrpc.OK
	return nil
}

// GetTribblesBySubscription retrieves a list of at most 100 tribbles posted by
// all users to which the specified UserID is subscribed in reverse chronological
// order (most recent first). Replies with status NoSuchUser if the specified UserID
// does not exist.
//type GetTribblesArgs struct {
//	UserID string
//}
//
//type GetTribblesReply struct {
//	Status   Status
//	Tribbles []Tribble
//}
func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {

	// check that user exists
	if _, err := ts.Lib.Get(args.UserID + ":create"); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// get User's subscripton list
	subs, err := ts.Lib.GetList(args.UserID + ":sub")
	if err != nil {
		// no subscriptions yet
		reply.Tribbles = make([]tribrpc.Tribble, 0)
		reply.Status = tribrpc.OK
		return nil
	}

	// get all tribbleIDs for users on User's subscripton list

	localMap := make(map[string]([]string))
	var totalLength int = 0
	for i := 0; i < len(subs); i++ {
		newTribbleIDs, err := ts.Lib.GetList(subs[i] + ":tribble")
		if err == nil {
			localMap[subs[i]] = newTribbleIDs
			totalLength += len(localMap[subs[i]])
		}
	}

	tribbleIDs := make([]string, totalLength)
	if totalLength == 0 {
		// no tribbles.
		reply.Tribbles = make([]tribrpc.Tribble, 0)
		reply.Status = tribrpc.OK
		return nil
	} else {
		var start int = 0
		for k, _ := range localMap {
			temp := localMap[k]
			for index := 0; index < len(temp); index++ {
				tribbleIDs[start] = temp[index]
				start += 1
			}
		}

		// sort the user's tribbleIDs in reverse chronological order
		sort.Sort(sort.Reverse(sort.StringSlice(tribbleIDs)))

		// allocate space for Tribbles
		numTribs := 100
		if len(tribbleIDs) < 100 {
			numTribs = len(tribbleIDs)
		}
		tribbles := make([]tribrpc.Tribble, numTribs)

		// get latest Tribbles from storageServer and unmarshall them
		for i := 0; i < numTribs; i++ {
			unmarshalled, err := ts.Lib.Get(tribbleIDs[i])
			if err != nil {
				return err
			}
			err = json.Unmarshal([]byte(unmarshalled), &(tribbles[i]))
			if err != nil {
				return err
			}
		}

		reply.Tribbles = tribbles
		reply.Status = tribrpc.OK
		return nil
	}
}
