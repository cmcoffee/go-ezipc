package blab

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
)

// This establishes what will be our source address to uplinks & peers.
var myAddr = fmt.Sprintf("%d.%s", genTag(), func() string {
	e := strings.Split(os.Args[0], "/")
	return e[len(e)-1]
}())

// Handles routing of recieved messages.
func (r *router) switchboard(req *msg) {

	// Negative TAGs are system calls, or a request to get status for a caller.
	if req.Tag == 0 {
		r.add_route(req.Src, req.conn)
		if r.uplink != nil && r.uplink != req.conn {
			r.uplink.send(req)
		}
		return
	} else if req.Tag < 0 {
		if r.busyCheck(req) {
			return
		}
	}

	// See if message is response to earlier request.
	if req.Dst == myAddr {
		if ok := r.intercept(req); ok {
			return
		}
	}

	// Check connection map to see if we need to map this elsewhere.
	if dest := r.find_route(req.Dst); dest != nil {
		err := r.route(req)
		if err != nil {
			r.send_err(req, err)
		}
		return
	}

	// End of the line, tell Caller we've failed to route the request.
	if req.Err == "" {
		dst := req.Dst
		req.Dst = req.Src
		req.Src = dst
		if req.Tag < 0 {
			req.Tag = req.Tag * -1
		}
		req.Err = ErrFail.Error()
		r.route(req)
		return
	}

	// Messages past this point are either invalid or expired.
	r.log.Printf("[Dropped] Dst:%s Src:%s Err:%s\n", req.Dst, req.Src, req.Err)
}

// Generates new *riphub.connection from net.Conn.
func (r *router) addconnection(conn net.Conn) *connection {
	return &connection{
		conn:   conn,
		router:   r,
		routes: make([]string, 0),
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
		fmt.Sprintf("%s\x1f%s\x1f%s\x1f%d\x1f%s\x1f%s\x04",
			req.Dst, req.Src, req.Err, req.Tag, req.Va1, req.Va2)))
	if err != nil && strings.Contains(err.Error(), "use of closed network connection") {
		return ErrClosed
	}
	return
}

// Sends error message to switchboard.
func (r *router) send_err(req *msg, err error) {
		dst := req.Dst
		req.Dst = req.Src
		req.Src = dst
		req.Va1 = ""
		req.Va2 = ""
		req.Err = err.Error()
		req.conn = r.find_route(req.Dst)
		if req.conn == nil {
			if r.uplink != nil {
				req.conn = r.uplink
			}
		} else {
			req.conn.send(req)
		} 
		r.log.Printf("[Dropped] Dst:%s Src:%s Err:%s\n", req.Dst, req.Src, req.Err)
}

// Routes messages to address or function.
func (s *router) route(req *msg) (err error) {
	conn := s.find_route(req.Dst)

	if req.Src == "" {
		req.Src = myAddr
	}

	if conn == nil {
		if s.uplink != nil {
			conn = s.uplink
		} else {
			return ErrClosed
		}
	}

	if conn.exec != nil {
		// If this is a bounceback, lets drop it.
		if req.Err != "" {
			return
		}
		if req.Err == "" && req.Tag > 0 {
			go func() {
				s.setBusy(req)
				err := req.conn.send(conn.exec(req))
				if err != nil {
					s.send_err(req, err)
				}
				s.unsetBusy(req)
			}()
			return
		}
	} else {
		return conn.send(req)
	}

	return ErrFail
}

// Adds tag from source to map, caller can then check on status of request.
func (s *router) setBusy(req *msg) {
	s.tagMapLock.Lock()
	defer s.tagMapLock.Unlock()
	if _, ok := s.tagMap[req.Tag * -1]; ok {
		s.send_err(req, ErrBadTag)
	} else {
		s.tagMap[req.Tag * -1] = struct{}{}
	}
	return
}

// Removes tag from source, after function completion.
func (s *router) unsetBusy(req *msg) {
	s.tagMapLock.Lock()
	defer s.tagMapLock.Unlock()
	if _, ok := s.tagMap[req.Tag * -1]; ok {
		delete(s.tagMap, req.Tag * -1)
	}
}

// Checks map to see if called function is currently still in process.
func (s *router) busyCheck(req *msg) bool {
	s.tagMapLock.RLock()
	defer s.tagMapLock.RUnlock()
	if _, ok := s.tagMap[req.Tag]; ok {
		return true
	}
	return false
}

// Adds alias to conneciton map.
func (r *router) add_route(addr string, conn *connection) {
	if conn.name == "" {
		conn.name = addr
	}
	atomic.StoreUint32(&r.ready, 1)
	r.connMapLock.Lock()
	defer r.connMapLock.Unlock()
	r.connMap[addr] = conn
	conn.routes = append(conn.routes, addr)
}

// Searches connection map for name specified.
func (r *router) find_route(addr string) *connection {
	r.connMapLock.RLock()
	defer r.connMapLock.RUnlock()
	return r.connMap[addr]
}
