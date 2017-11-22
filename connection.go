package blab

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// IPC Connection.
type connection struct {
	conn     net.Conn
	router   *router
	routes   []string
	err      error
	sendLock sync.Mutex
	exec     func(*msg) *msg
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

// Drops outgoing message to out queue for delivery to connection.
func (c *connection) send(req *msg) (err error) {
	if c.router.Debug {
		if req.Tag != 0 {
			c.debugRequest("Send", req)
		}
	}
	c.sendLock.Lock()
	defer c.sendLock.Unlock()
	_, err = c.conn.Write([]byte(
		fmt.Sprintf("%d\x1f%s\x1f%s\x1f%s\x1f%s\x04",
			req.Tag, req.Dst, req.Err, req.Va1, req.Va2)))
	if err != nil && req.Err != "" {
		return
	}
	if err != nil {
		if c.router.Debug {
			fmt.Printf("Send failed: %s\n", err.Error())
		}
		if req.conn == nil {
			return ErrClosed
		}
		send_err(req, ErrClosed)
	}
	return
}

// Logs sending and receiving of messages.
func (c connection) debugRequest(prefix string, request *msg) {
	if request.Tag == 0 {
		fmt.Printf("[%s] Route announcement for \"%s\"\n", prefix, request.Dst)
	} else {
		fmt.Printf("[%s] Dst:%s, Tag:%d, Err:%s\n", prefix, request.Dst, request.Tag, request.Err)
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
			c.err = c.reciever()
		}()
		return nil
	}
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
