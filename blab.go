/*
Package blab provides a means of creating a network of methods/functions.

Callers include producers and consumers, producers may also connect to other producers.
This first Caller to "Listen" doubles as a broker to direct all Calls.

Once a Call is placed the first Caller/Listener will route the call to the appropriate producer who registered the method or function.

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
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"fmt"
)

var ErrFail = errors.New("Request failed, service unavailable.")
var ErrClosed = errors.New("Connection closed.")

// Message Packet.
type msg struct {
	Dst string
	Src string
	Tag int32
	Err string
	Va1 string
	Va2 string
	conn *connection
}

// IPC router.
type router struct {
	// the socket file.
	socketf string
	// uplink is used to designate our dispatcher.
	uplink *connection
	// log is used for logging routing errors.
	log *log.Logger
	// tagMap is for keeping track of requests.
	tagMap     map[int32]interface{}
	tagMapLock sync.RWMutex
	// connMap keeps track of all routes that we can send from, if not matched here, send to uplink if avaialble, send Err if not.
	connMap     map[string]*connection
	connMapLock sync.RWMutex
	// Flag to tell if connection is established and ready.
	ready     uint32
	// Determines if we are a client or a server.
	is_client bool
	// Limits ammount of open connections.
	limiter chan struct{}
	// Sets debug flag for traffic monitoring.
	Debug bool
}

// IPC Connection.
type connection struct {
	name     string
	conn     net.Conn
	router   *router
	routes   []string
	sendLock sync.Mutex
	exec     func(*msg) *msg
}

// Logs sending and receiving of messages.
func (c connection) debugRequest(prefix string, request *msg) {
	if request.Tag == 0 {
		if c.name == "" {
			c.router.log.Printf("[%s] %s Greetings!\n", request.Src, prefix)
		} else {
			c.router.log.Printf("[%s] %s Route announcement for \"%s\"\n", c.name, prefix, request.Src)
		}
	} else {
		c.router.log.Printf("[%s] %s - Dst:%s, Src:%s, Tag:%d, Err:%s\n", c.name, prefix, request.Dst, request.Src, request.Tag, request.Err)
	}
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
	if err != nil {
		return
	}
	out.Tag = int32(tag)

	out.Va1 = msgPart[4]
	out.Va2 = msgPart[5]

	return
}

// Sets connection limit for maximum IPC connections.
func (r *router) SetLimiter(connlimit int) {
	r.limiter = make(chan struct{}, connlimit)
	for i := 0; i < connlimit; i++ {
		r.limiter <- struct{}{}
	}
}

// Creates a new blab router.
func New() *router {
	r := &router{
		uplink:   nil,
		log:      log.New(os.Stdout, "", log.LstdFlags),
		tagMap:   make(map[int32]interface{}, 128),
		connMap:  make(map[string]*connection, 128),
		Debug: false,
	}
	r.SetLimiter(256)
	return r
}

// Directs error output to a specified io.Writer, defaults to os.Stdout.
func (r *router) SetLogWriter(w io.Writer) {
	r.log = log.New(w, "", 0)
	return
}

// Creates socket file(socketf) connection to Broker, forks goroutine and returns. (consumers)
func (r *router) Dial(socketf string) error {
	r.is_client = true
	return r.open(socketf)
}


// Listens to socket files(socketf) for clients.
func (r *router) Listen(socketf string) (err error) {
	r.is_client = false

	// Attempt to open socket file, if this works, stop here and serve.
	err = r.open(socketf)
	if err == nil || !strings.Contains(err.Error(), "connection refused") && !strings.Contains(err.Error(), "no such file or directory") {
		return err
	}

	r.socketf = socketf

	// Clean out old socket files.
	s_split := strings.Split(socketf, "/")
	if len(s_split) == 0 {
		return fmt.Errorf("%s: incomplete path to socket file.", socketf)
	}
	sfile_name := s_split[len(s_split)-1]
	path := strings.Join(s_split[0:len(s_split)-1], "/")

	files, err := ioutil.ReadDir(path)
	if err != nil {
		return
	}
	for _, file := range files {
		fname := file.Name()
		if strings.Contains(fname, sfile_name) {
			os.Remove(path + "/" + fname)
		}
	}

	l, err := net.Listen("unix", socketf)
	if err != nil {
		return err
	}

	for {
		<-r.limiter
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		c := r.addconnection(conn)

		// Spin connection off to go thread.
		go func() {
			err = c.reciever()
			if err != ErrClosed {
				r.log.Println(err)
				r.limiter <- struct{}{}
				return
			}
			r.limiter <- struct{}{}
		}()
	}
}

// Creates socket connection to file(socketf) launches listener, forks goroutine or blocks depending on method wrapper called.
func (r *router) open(socketf string) error {
	conn, err := net.Dial("unix", socketf)
	if err != nil {
		return err
	}
	c := r.addconnection(conn)

	r.socketf = socketf
	r.uplink = c

	var done uint32
	atomic.StoreUint32(&done, 1)

	// If this is a service, we'll return the actual listener, if not push to background.
	if !r.is_client {
		return c.reciever()
	} else {
		go func() {
			if err := c.reciever(); err != nil && err != ErrClosed {
				r.log.Printf("[Receiver Error] %v\n", err)
				
			}
		}()
		return nil
	}
}

// Finds split in message when two messages are concatinated together.
func findSplit(in []byte) (n int) {
	for _, ch := range in {
		if ch == '\x04' {
			return n
		}
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
	c.send(&msg{
		Src: myAddr,
		Tag: 0,
	})

	if c.router.uplink != nil {
		c.router.connMapLock.RLock()
		for name, _ := range c.router.connMap {
			c.send(&msg{
				Src: name,
				Tag: 0,
			})
		}
		c.router.connMapLock.RUnlock()
	}

	// Reciever loop for incoming messages.
	for {
		for n, _ := range input {
			input[n] = 0
		}
		input = inbuf[0:]

		sz, err = c.conn.Read(input)
		if err != nil {
			c.close()
			if err == io.EOF {
				err = ErrClosed
			}
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
			if c.router.Debug {
				c.debugRequest("Recv", request)
			}

			request.conn = c

			c.router.switchboard(request)

			if len(pbuf)-s > 1 {
				pbuf = pbuf[s+1:]
				continue
			}
			pbuf = nil
		}
	}
	return
}
