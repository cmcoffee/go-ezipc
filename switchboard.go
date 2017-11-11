package blab

import (
	"encoding/json"
)

const (
	regSelf = -1
	regAddr = -2
)

// Handles routing of recieved messages.
func (s *session) switchboard(c *connection, req *msg) {

	// Negative TAGs are system calls, or a request to get status for a caller.
	if req.Tag < 0 {
		switch req.Tag {
		case regSelf:
			json.Unmarshal(req.Va1, &c.id)
			s.add_route(c.id, c)
			return
		case regAddr:
			s.register(req)
			return
		default:
			if s.busyCheck(req) {
				return
			}
		}
	}

	// Add route if it is not arriving from Dispatcher.
	if src := s.find_route(req.Src); src == nil && c != s.uplink {
		s.add_route(req.Src, c)
	}

	// See if message is response to earlier request.
	if req.Dst == myAddr {
		if ok := s.intercept(req); ok {
			return
		}
	}

	// Check connection map to see if we need to map this elsewhere.
	if dest := s.find_route(req.Dst); dest != nil {
		if err := s.route(req); err != nil {
			s.log.Println(err)
		}
		return
	}

	// Check local functions to see if we can answer Call.
	if req.Err == "" && req.Tag > 0 {
		s.localMapLock.RLock()
		if dst, ok := s.localMap[req.Dst]; ok {
			s.localMapLock.RUnlock()

			// Send message to function.
			s.setBusy(req)

			if err := s.route(dst(req)); err != nil {
				s.log.Println(err)
			}
			s.unsetBusy(req)

			return
		}
		s.localMapLock.RUnlock()
	}

	// End of the line, tell Caller we've failed to route the request.
	if req.Err == "" {
		dst := req.Dst
		req.Dst = req.Src
		req.Src = dst
		req.Va1 = nil
		if req.Tag < 0 {
			req.Tag = req.Tag * -1
		}
		req.Err = ErrFail.Error()
		if err := s.route(req); err != nil {
			s.log.Println(err)
		}
	}
	// Messages past this point are either invalid or expired.
}
