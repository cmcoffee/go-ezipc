package blab

import (
	"sync"
)

// IPC router.
type router struct {
	// the socket file.
	socketf string
	// uplink is used to designate our dispatcher.
	uplink *connection
	// tagMap is for keeping track of requests.
	tagMap     map[int32]*bucket
	tagMapLock sync.RWMutex
	// connMap keeps track of all routes that we can send from, if not matched here, send to uplink if avaialble, send Err if not.
	connMap     map[string]*connection
	connMapLock sync.RWMutex
	// Determines if we are a client or a server.
	is_client bool
	// Limits ammount of open connections.
	limiter chan struct{}
	// Sets debug flag for traffic monitoring.
	Debug bool
}

// Message Packet.
type msg struct {
	Tag int32
	Dst string
	Err string
	Va1 string
	Va2 string
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
func (r *router) route(req *msg) {
	var tag int32
	var target *bucket

	tag = req.Tag

	if req.Tag < 0 {
		tag = tag * -1
	} 

	// Register functions with reserved tag=0.
	if tag == 0 {
		r.connMapLock.Lock()
		defer r.connMapLock.Unlock()
		r.connMap[req.Dst] = req.conn
		req.conn.routes = append(req.conn.routes, req.Dst)
		if r.uplink != nil && req.conn != r.uplink {
			r.uplink.send(req)
		}
		return
	} else {
		r.tagMapLock.RLock()
		target = r.tagMap[tag]
		r.tagMapLock.RUnlock()
	}

	// Process existing bucket with tag identifier.
	if target != nil {
		switch target.flag {
			case REQUEST:
				// If this a return message handle it.
				if req.conn == target.dst || req.conn == nil {
					r.intercept(req)
				} else {
					// We already have a bucket that does not match.
					send_err(req, ErrBadTag)
				}
			case RELAY:
				// Relay message to end point.
				if req.conn == target.src {
					target.dst.send(req)
				} else if req.conn == target.dst {
					target.src.send(req)
					r.tagMapLock.Lock()
					delete(r.tagMap, tag)
					r.tagMapLock.Unlock()
				} else {
					send_err(req, ErrBadTag)
				}
			default:
				if req.conn == target.src {
					return
				} else {
					send_err(req, ErrBadTag)
				}
		}
	} else {
		// Create local tag after looking up destination.
		r.connMapLock.RLock()
		dest := r.connMap[req.Dst]
		r.connMapLock.RUnlock()
		if dest == nil {
			send_err(req, ErrFail)
			return
		}

		// Create bucket for handling end point or relay.
		nb := new(bucket)
		if dest.exec != nil {
			nb.flag = END
			nb.src = req.conn
		} else {
			nb.flag = RELAY
			nb.src = req.conn
			nb.dst = dest
		}
		
		r.tagMapLock.Lock()
		r.tagMap[tag] = nb
		r.tagMapLock.Unlock()

		// Execute local function if possible.
		if dest.exec != nil {
			req.conn.send(dest.exec(req))
			r.tagMapLock.Lock()
			delete(r.tagMap, tag)
			r.tagMapLock.Unlock()
		} else {
			dest.send(req)
		}
	}
	return 

}