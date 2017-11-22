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
	"io/ioutil"
	"net"
	"os"
	"strings"
	"fmt"
)

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
		//logg:      log.New(os.Stdout, "", log.LstdFlags),
		tagMap:   make(map[int32]*bucket),
		connMap:  make(map[string]*connection),
		Debug: false,
	}
	r.SetLimiter(256)
	return r
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
			c.err = c.reciever()
			r.limiter <- struct{}{}
		}()
	}
}