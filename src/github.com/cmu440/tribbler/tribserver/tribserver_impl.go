package tribserver

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
)

type tribServer struct {
	Lib libstore.Libstore //Libstore of the tribserver
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

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	fmt.Printf("CreateUser: got message from tribclient\n")
	libstore.HandleCreateUser()
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	return errors.New("not implemented")
}
