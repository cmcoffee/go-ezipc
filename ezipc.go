/*
Package ezipc provides a simple framework to allow for communication between multiple background processes.

EzIPC has similar requirements for exporting functions that Go's native "rpc" package provides, however ezipc maps both functions and object methods.

	1)the method or function requires two arguments, both exported (or builtin) types.
	2)the method or function's second argument is a pointer.
	3)the method or function has a return type of error.

Registered methods or functions should look like:

func (*T) Name(argType T1, replyType *T2) error

	and respectively ...

func name(argType T1, replyType *T2) error

Exported functions & methods should be made thread safe.

*/
package ezipc

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Creates a new ezipc router.
func New() *EzIPC {
	r := &EzIPC{
		uplink:  nil,
		tagMap:  make(map[int32]*bucket),
		connMap: make(map[string]*connection),
	}
	return r
}

// EzIPC is common to both IPC clients and servers.
type EzIPC struct {
	// the socket file.
	socketf string
	// uplink is used to designate our dispatcher.
	uplink *connection
	// tagMap is for keeping track of requests.
	tagMap     map[int32]*bucket
	tagMapLock sync.Mutex
	// connMap keeps track of all routes that we can send from, if not matched here, send to uplink if avaialble, send Err if not.
	connMap     map[string]*connection
	connMapLock sync.RWMutex
	// Determines if we are a client or a server.
	is_client bool
}

// EzIPC Connection.
type connection struct {
	conn     net.Conn
	router   *EzIPC
	routes   []string
	err      error
	sendLock sync.Mutex
	exec     func(*msg) *msg
}

// Creates socket connection to file(socketf) and communicates with othe processes, blocks for listeners, runs go routine for clients.
func (e *EzIPC) open(socketf string) error {
	conn, err := net.Dial("unix", socketf)
	if err != nil {
		return err
	}
	c := e.addconnection(conn)

	e.socketf = socketf
	e.uplink = c

	var done uint32
	atomic.StoreUint32(&done, 1)

	// If this is a service, we'll return the actual listener, if not push to background.
	if !e.is_client {
		return c.reciever()
	} else {
		go func() {
			c.err = c.reciever()
		}()
		return nil
	}
}

// Closes connection
func (c *connection) close() (err error) {
	c.router.connMapLock.Lock()
	for _, name := range c.routes {
		delete(c.router.connMap, name)
	}
	c.router.connMapLock.Unlock()

	err = c.conn.Close()
	return
}

// Sends *msg to specific connection.
func (c *connection) send(req *msg) (err error) {
	c.sendLock.Lock()
	defer c.sendLock.Unlock()
	_, err = c.conn.Write([]byte(
		fmt.Sprintf("%d\x1f%s\x1f%s\x1f%s\x1f%s\x04",
			req.Tag, req.Dst, req.Err, req.Va1, req.Va2)))
	if err != nil && req.Err != "" {
		return
	}
	if err != nil {
		if req.conn == nil {
			return ErrClosed
		}
		send_err(req, ErrClosed)
	}
	return
}

// Listens to *connection, decodes msg's and passes them to switchboard.
func (c *connection) reciever() (err error) {
	inbuf := make([]byte, 1024)
	input := inbuf[0:]

	var sz int
	var pbuf []byte

	// Register Names
	if c.router.uplink != nil {
		c.router.connMapLock.RLock()
		for name, _ := range c.router.connMap {
			c.send(&msg{
				Dst: name,
				Tag: 0,
			})
		}
		c.router.connMapLock.RUnlock()
	}

	// Finds field delimeters in byte stream.
	findSplit := func(in []byte) (n int) {
		for _, ch := range in {
			if ch == '\x04' {
				return n
			}
			n++
		}
		return n
	}

	// Decodes bytes to message.
	decMessage := func(in []byte) (out *msg, err error) {
		msgPart := strings.Split(string(in), "\x1f")

		if len(msgPart) < 5 {
			return nil, fmt.Errorf("Incomplete or corrupted message: %s", string(in))
		}

		tag, err := strconv.ParseInt(msgPart[0], 0, 32)
		if err != nil {
			return
		}
		out = &msg{
			Tag: int32(tag),
			Dst: msgPart[1],
			Err: msgPart[2],
			Va1: msgPart[3],
			Va2: msgPart[4],
		}
		return
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
			s := findSplit(pbuf)

			var request *msg

			request, err = decMessage(pbuf[0:s])
			if err != nil {
				c.close()
				return
			}

			request.conn = c
			c.router.route(request)

			if len(pbuf)-s > 1 {
				pbuf = pbuf[s+1:]
				continue
			}
			pbuf = nil
		}
	}
	return
}

// Message Packet.
type msg struct {
	Tag  int32
	Dst  string
	Err  string
	Va1  string
	Va2  string
	conn *connection
}

// Sends error message to switchboard.
func send_err(req *msg, err error) {
	req.Va1 = ""
	req.Va2 = ""
	req.Err = err.Error()
	if req.conn != nil {
		req.conn.send(req)
	}
}

// Reads each incoming message, records tag, process and sends to appropriate destination.
func (e *EzIPC) route(req *msg) {
	var tag int32
	var target *bucket

	tag = req.Tag

	if req.Tag < 0 {
		tag = tag * -1
	}

	// Register functions with reserved tag=0.
	if tag == 0 {
		e.connMapLock.Lock()
		defer e.connMapLock.Unlock()
		e.connMap[req.Dst] = req.conn
		req.conn.routes = append(req.conn.routes, req.Dst)
		if e.uplink != nil && req.conn != e.uplink {
			e.uplink.send(req)
		}
		return
	} else {
		e.tagMapLock.Lock()
		defer e.tagMapLock.Unlock()
		target = e.tagMap[tag]
	}

	// Process existing bucket with tag identifier.
	if target != nil {
		switch target.flag {
		case t_REQUEST:
			// If this a return message handle it.
			if req.conn == target.dst || req.conn == nil {
				if bucket, ok := e.tagMap[req.Tag]; ok {
					bucket.data = req
					bucket.done <- struct{}{}
					delete(e.tagMap, req.Tag)
					return
				}
			} else {
				// Duplicate TAG requested, send error back.
				send_err(req, errBadTag)
			}
		case t_RELAY:
			// Relay message to end point.
			if req.conn == target.src {
				target.dst.send(req)
			} else if req.conn == target.dst {
				target.src.send(req)
				delete(e.tagMap, tag)
			} else {
				send_err(req, errBadTag)
			}
		default:
			if req.conn == target.src {
				return
			} else {
				send_err(req, errBadTag)
			}
		}
	} else {
		// Create local tag after looking up destination.
		e.connMapLock.RLock()
		dest := e.connMap[req.Dst]
		e.connMapLock.RUnlock()
		if dest == nil {
			send_err(req, ErrFail)
			return
		}

		// Create bucket for handling end point or relay.
		nb := new(bucket)
		if dest.exec != nil {
			nb.flag = t_EXEC
			nb.src = req.conn
		} else {
			nb.flag = t_RELAY
			nb.src = req.conn
			nb.dst = dest
		}

		e.tagMap[tag] = nb

		// Execute local function if possible.
		if dest.exec != nil {
			req.conn.send(dest.exec(req))
			delete(e.tagMap, tag)
		} else {
			dest.send(req)
		}
	}
	return

}

// Dial is the client function of EzIPC, it opens a connection to the socket file.
func (e *EzIPC) Dial(socketf string) error {
	e.is_client = true
	return e.open(socketf)
}

// Listens is the server function of EzIPC, it opens a connection and blocks while listening for requests.
func (e *EzIPC) Listen(socketf string) (err error) {
	e.is_client = false

	// Attempt to open socket file, if this works, stop here and serve.
	err = e.open(socketf)
	if err == nil || !strings.Contains(err.Error(), "connection refused") && !strings.Contains(err.Error(), "no such file or directory") {
		return err
	}

	e.socketf = socketf

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
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		c := e.addconnection(conn)

		// Spin connection off to go thread.
		go func() {
			c.err = c.reciever()
		}()
	}
}
