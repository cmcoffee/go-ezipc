/*
Package blab provides a means of creating a network of methods/functions.

Callers include producers and consumers, producers may also connect to other producers.
This first Caller to "Listen" doubles as a broker to direct all Calls.
Callers create their own socket file for direct connections to other Callers.

Once a Call is placed the first Caller/Listener will route the call to the appropriate producer who registered the method or function.
After the reply is sent, the producer will attempt to create a direct connection to the consumer for direct answering of any subsiquent calls.

blab has similar requirements for exporting functions that Go's native "rpc" package provides, however blab maps both functions and object methods.

	1)the method or function requires two arguments, both exported (or builtin) types.
	2)the method or function's second argument is a pointer.
	3)the method or function has a return type of error.

Registered methods or functions should look like:

func (*T) Name(argType T1, replyType *T2) error

	and respectively ...

func name(argType T1, replyType *T2) error

Exported functions & methods should be made thread safe.

*/
package blab

import (
	"fmt"
	"errors"
	"net"
	"bytes"
	"io"
	"os"
	"log"
	"sync"
	"runtime"
	"strings"
	"strconv"
	"encoding/base64"
	"encoding/json"
	"sync/atomic"
	"io/ioutil"
)

var ErrFail = errors.New("Call to unknown method or function.")
var ErrClosed = errors.New("Connection closed.")

// Message Packet.
type msg struct {
	Dst string 
	Src string
	Tag int32 
	Err string
	Va1 []byte
	Va2 []byte
}

// The Caller object is for both producers(registering methods/functions) and consumers(calls registered methods/functions).
type Caller struct { 
	*session
	fork bool
}

// IPC Session.
type session struct {
	// the socket file.
	socketf string
	// uplink is used to designate our dispatcher.
	uplink *connection
	// log is used for logging errors generated from background routines.
	log *log.Logger
	// reqMap is for outbound request handling.
	reqMap map[int32]*bucket
	reqMapLock sync.RWMutex
	// localMap is for local methods/function lookup and execution.
	localMap map[string]func(*msg)(*msg)
    localMapLock sync.RWMutex
    // busyMap is so a Caller can request the status of a function already being fulfilled.
	busyMap map[string]map[int32]struct{}
	busyMapLock sync.RWMutex
	// connMap keeps track of all routes that we can send from, if not matched here, send to uplink if avaialble, send Err if not.
	connMap map[string]*connection
	connMapLock sync.RWMutex
	// peerLock is used to prevent multiple peer connections going to the same location.
	peerLock int32
	// ServeNode
	serveNode *Caller
}

// IPC Connection.
type connection struct {
	conn net.Conn
	enc *json.Encoder
	sess *session
	id string
	routes []string
	sendLock sync.Mutex

}

// Decodes msg.
func decMessage(in []byte) (out *msg, err error) {

	msgPart := strings.Split(string(in), "\x1f")
	if len(msgPart) < 6 {
		return nil, fmt.Errorf("Incomplete or corrupted message: %s", string(in))
	}

	out = &msg{
		Dst: msgPart[0],
		Src: msgPart[1],
		Err: msgPart[2],
	}

	tag, err := strconv.ParseInt(msgPart[3], 0, 32)
	if err != nil { return }
	out.Tag = int32(tag)

	va1, err := base64.StdEncoding.DecodeString(msgPart[4])
	if err != nil { return }
	out.Va1 = va1

	va2, err := base64.StdEncoding.DecodeString(msgPart[5])
	if err != nil { return }
	out.Va2 = va2

	return
}

// Allocates new Caller.
func NewCaller() *Caller {
	return &Caller{
		newSession(),
		true,
	}
}

// Creates a new blab session.
func newSession() *session {
	return &session{
		uplink: nil,
		log: log.New(os.Stdout, "", log.LstdFlags),
		reqMap: make(map[int32]*bucket),
		localMap: make(map[string]func(*msg)(*msg)),
		busyMap: make(map[string]map[int32]struct{}),
		connMap: make(map[string]*connection),
	}
}

// Directs error output to a specified io.Writer, defaults to os.Stdout.
func (cl *Caller) SetOutput(w io.Writer) {
	if cl.log == nil { *cl = *NewCaller() }
	cl.log = log.New(w, "", 0)
	if cl.serveNode != nil { cl.serveNode.log = cl.log }
	return 
}

// Listens to socket files(socketf) for Callers.
// If socketf is not open, Listen opens the file and connects itself to it. (producers)
func (cl *Caller) Listen(socketf string) (err error) {
	cl.fork = false
	err = cl.open(socketf)
	if err == nil || !strings.Contains(err.Error(), "connection refused") && !strings.Contains(err.Error(), "no such file or directory") {
		return err
	}

	if cl.session == nil { *cl = *NewCaller() }

	server := NewCaller()

	server.socketf = socketf
	server.log = cl.log
	cl.socketf = socketf

	// Clean out old socket files.
	s_split := strings.Split(socketf, "/")
	if len(s_split) == 0 { return fmt.Errorf("%s: incomplete path to socket file.", socketf)}
	sfile_name := s_split[len(s_split) - 1]
	path := strings.Join(s_split[0:len(s_split) - 1], "/")

	files, err := ioutil.ReadDir(path)
	if err != nil { return }
	for _, file := range files {
		fname := file.Name()
		if strings.Contains(fname, sfile_name) {
			os.Remove(path + "/" + fname)
		}
	}

	l, err := net.Listen("unix", socketf)
	if err != nil { return err }

	err = cl.Dial(socketf)
	if err != nil { return err }
	
	cl.serveNode = server

	for {
		conn, err := l.Accept()
		if err != nil { return err }
		c := server.addconnection(conn)
		
		go func () {
			err = c.reciever()
			if err != ErrClosed {
				cl.log.Println(err)
				return  
			}
		}()
	}	
}

// Creates socket file(socketf) connection to Broker, forks goroutine and returns. (consumers)
func (cl *Caller) Dial(socketf string) (error) {
	cl.fork = true
	return cl.open(socketf)
}

// Creates socket connection to file(socketf) launches listener, forks goroutine or blocks depending on method wrapper called.
func (cl *Caller) open(socketf string) (error) {
	if cl.session == nil { *cl = *NewCaller() }

	conn, err := net.Dial("unix", socketf)
	if err != nil { return err }
	c := cl.addconnection(conn)

	cl.socketf = socketf	
	cl.uplink = c

	var done uint32
	atomic.StoreUint32(&done, 1)

	// Open socket file for peer connections from Callers.
	go func() {
		defer func() {
			if r := recover(); r != nil {
				atomic.StoreUint32(&done, 0)
				cl.log.Println(r)
			}
		}()
		socketf = fmt.Sprintf("%s.%s", socketf, myAddr)
		os.Remove(socketf)
		l, err := net.Listen("unix", socketf)
		if err != nil { panic(err.Error()) }

		for {
			atomic.StoreUint32(&done, 0)
			conn, err := l.Accept()
			if err != nil { panic(err.Error()) }
			c := cl.addconnection(conn)
			go func () { 
				if err := c.reciever(); err != nil && err != ErrClosed { cl.log.Println(err) }
			}()
		}
	}()

	// Let the reciever socket open.
	for !atomic.CompareAndSwapUint32(&done, 0, 1) { runtime.Gosched() }

	// If this is a service, we'll return the actual listener, if not push to background.
	if !cl.fork {
		return c.reciever()
	} else {
		go func() {
			if err := c.reciever(); err != nil && err != ErrClosed { cl.log.Println(err) }
		}()
		return nil
	}
}

// Creates direct connection to caller to speed response time.
func (s *session) peerConnect(peer string) (error) {

	s.connMapLock.RLock()
	if _, ok := s.connMap[peer]; ok {
		s.connMapLock.RUnlock()
		return nil
	}
	s.connMapLock.RUnlock()


	if !atomic.CompareAndSwapInt32(&s.peerLock, 0, 1) { return nil }
	defer atomic.CompareAndSwapInt32(&s.peerLock, 1, 0)

	socketf := fmt.Sprintf("%s.%s", s.socketf, peer)

	conn, err := net.Dial("unix", socketf)
	if err != nil { 
		return err 
	}
	c := s.addconnection(conn)

	// Forgo logging on peer connection.
	go func () { 
		if err := c.reciever(); err != nil && err != ErrClosed { s.log.Println(err) }
	}()

	return nil
}

// Finds split in message when two messages are concatinated together.
func findSplit(in []byte) (n int) {
	for _, ch := range in {
		if ch == '\x04' { return n }
		n++
	}
	return n
}

// Listens to *connection, decodes msg's and passes them to switchboard.
func (c *connection) reciever() (err error) {
	inbuf := make([]byte, 1024)
	input := inbuf[0:]
	
	var sz int
	var pbuf []byte

	// Register all local functions with uplink or peer.

		data, _ := json.Marshal(myAddr)
		c.send(&msg {
			Tag: regSelf,
			Va1: data,
			})
		c.sess.localMapLock.RLock()
		for name, _ := range c.sess.localMap {
			data, _ := json.Marshal(name)
			c.send(&msg{
				Src: myAddr,
				Tag: regAddr,
				Va1: data,
			})
		}
		c.sess.localMapLock.RUnlock()

	// Reciever loop for incoming messages.
	for {
		for n, _ := range input {
			input[n] = 0
		}
		input = inbuf[0:]
		
		sz, err = c.conn.Read(input)
		if err != nil {
			c.close()
			if err == io.EOF { err = ErrClosed }
			return
		}

		pbuf = append(pbuf, input[0:sz]...)
		
		// \x1f used as a delimeter between messages.
		for bytes.Contains(pbuf, []byte("\x04")) {
			//sz := len(pbuf)

			s := findSplit(pbuf)

			var request *msg

			request, err = decMessage(pbuf[0:s])
			if err != nil {
				c.close()
				return
			}
			go c.sess.switchboard(c, request)

			if len(pbuf) - s > 1 {
				pbuf = pbuf[s + 1:]
				continue
			}
			pbuf = nil
		} 
	}
	return
}
