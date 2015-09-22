package blab

import (
	"fmt"
	"os"
	"net"
	"strings"
	"encoding/json"
	"encoding/base64"
)

// This establishes what will be our source address to uplinks & peers.
var myAddr = fmt.Sprintf("%d.%s", genTag(), func() string {
			e := strings.Split(os.Args[0], "/")
			return e[len(e) - 1]
		}())

// Generates new *riphub.connection from net.Conn.
func (s *session) addconnection(conn net.Conn) (*connection) {
	 return &connection{
		conn: conn,
		enc: json.NewEncoder(conn),
		sess: s,
		routes: make([]string, 0),
	}	
}

// Closes connection
func (c *connection) close() (err error) {
	os.Remove(fmt.Sprintf("%s.%s", c.sess.socketf, c.id))
	
	if c.sess.uplink == c { 
		c.sess.uplink = nil
		os.Remove(fmt.Sprintf("%s.%s", c.sess.socketf, myAddr))
	}
	
	c.sess.connMapLock.Lock()
	for _, name := range c.routes {
		delete(c.sess.connMap, name) 
	}
	c.sess.connMapLock.Unlock()

	c.sess.busyMapLock.Lock()
	delete(c.sess.busyMap, c.id)
	c.sess.busyMapLock.Unlock()
	
	err = c.conn.Close()
	return
}

// Routes outbound messages to address.
func (s *session) route(req *msg) (err error) {
	s.connMapLock.RLock()
	conn := s.find_route(req.Dst)
	s.connMapLock.RUnlock()

	if req.Src == "" { req.Src = myAddr }

	if conn == nil {
		if s.uplink != nil {
			conn = s.uplink
		} else {
			return ErrClosed
		}
	}
	return conn.send(req)
}

// Drops outgoing message to out queue for delivery to connection.
func (c *connection) send(req *msg) (err error) {
	c.sendLock.Lock()
	defer c.sendLock.Unlock()
	_, err = c.conn.Write([]byte(
		fmt.Sprintf("%s\x1f%s\x1f%s\x1f%d\x1f%s\x1f%s\x04", 
		req.Dst, req.Src, req.Err, req.Tag, 
		base64.StdEncoding.EncodeToString(req.Va1), 
		base64.StdEncoding.EncodeToString(req.Va2))))
	return
}

// Adds tag from source to map, caller can then check on status of request.
func (s *session) setBusy(req *msg) {
	s.busyMapLock.Lock()
	defer s.busyMapLock.Unlock()
	if _, ok := s.busyMap[req.Src]; !ok {
		s.busyMap[req.Src] = make(map[int32]struct{})
	}
	s.busyMap[req.Src][req.Tag] = struct{}{}
}

// Removes tag from source, after function completion.
func (s *session) unsetBusy(req *msg) {
	s.busyMapLock.Lock()
	defer s.busyMapLock.Unlock()
	if _, ok := s.busyMap[req.Src]; !ok { return }
	delete(s.busyMap[req.Src], req.Tag)
}

// Checks map to see if called function is currently still in process.
func (s *session) busyCheck(req *msg) bool {
	s.busyMapLock.RLock()
	defer s.busyMapLock.RUnlock()
	tag := req.Tag * -1
	if src, ok := s.busyMap[req.Src]; ok {
		if _, ok := src[tag]; ok {
			return true
		}
	}
	return false
}

// Adds alias to conneciton map.
func (s *session) add_route(addr string, conn *connection) {
	s.connMapLock.Lock()
	defer s.connMapLock.Unlock()
	s.connMap[addr] = conn
	conn.routes = append(conn.routes, addr)
}

// Searches connection map for name specified.
func (s *session) find_route(addr string) *connection {
	s.connMapLock.RLock()
	defer s.connMapLock.RUnlock()
	return s.connMap[addr]
}
