package blab

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"reflect"
	"time"
)

const (
	REQUEST = 1 << iota
	RELAY
	END
)

type bucket struct {
	flag int
	done chan struct{}
	data *msg
	dst  *connection
	src  *connection
}

var ErrFail = errors.New("Request failed, service unavailable.")
var ErrBadTag = errors.New("Duplicate tag detected.")
var ErrClosed = errors.New("Connection closed.")

// Call invokes a registered method/function, blocks while actively checking for for completion, returns err on failure.
func (r *router) Call(name string, arg interface{}, reply interface{}) (err error) {
	data, err := json.Marshal(arg)
	if err != nil {
		return err
	}

	data2, err := json.Marshal(reply)
	if err != nil {
		return err
	}

	dest := r.uplink

new_request:
	if dest == nil {
		r.connMapLock.RLock()
		dest = r.connMap[name]
		r.connMapLock.RUnlock()
	}

	if dest == nil {
		return ErrClosed
	}

	bucket, tag := r.getBucket()
	bucket.data = nil
	bucket.dst = dest

	dest.send(&msg{
		Dst: name,
		Va1: base64.StdEncoding.EncodeToString(data),
		Va2: base64.StdEncoding.EncodeToString(data2),
		Tag: tag,
	})
	if err != nil {
		return err
	}

	// Remove bucket from map.
	reset_bucket := func() {
		r.tagMapLock.Lock()
		delete(r.tagMap, tag)
		r.tagMapLock.Unlock()
	}

	for {
		select {
		// Once request is met, provide result and/or error to Caller.
		case <-bucket.done:
			if len(bucket.data.Va2) > 0 && reflect.ValueOf(reply).Kind() == reflect.Ptr {

				var va2 []byte
				va2, err = base64.StdEncoding.DecodeString(bucket.data.Va2)
				if err != nil {
					return
				}

				err = json.Unmarshal(va2, reply)
				if err != nil && err != io.EOF {
					return
				}
			}

			switch bucket.data.Err {
			case ErrBadTag.Error():
				reset_bucket()
				goto new_request
			case ErrFail.Error():
				err = ErrFail
			default:
				if bucket.data.Err != "" {
					err = errors.New(bucket.data.Err)
				}
			}
			reset_bucket()
			return

		// Send busyCheck to see if we should continue waiting on reply.
		case <-time.After(time.Millisecond * 300):
			if dest == nil {
				return ErrClosed
			}
			err = dest.send(&msg{
				Dst: name,
				Tag: tag * -1,
			})
			if err != nil {
				return err
			}
			continue
		}
	}
	return
}

// Checks to see if this is a response to a Call we've placed.
func (s *router) intercept(req *msg) bool {
	s.tagMapLock.Lock()
	defer s.tagMapLock.Unlock()
	if bucket, ok := s.tagMap[req.Tag]; ok {
		bucket.data = req
		bucket.done <- struct{}{}
		delete(s.tagMap, req.Tag)
		return true
	}
	return false
}

// Assigned Call a bucket to capture reply with.
func (r *router) getBucket() (*bucket, int32) {

	// Creates a random 32bit tag for IPC calls.
	genTag := func() int32 {
		maxBig := *big.NewInt(int64(1<<31 - 1))
		output, _ := rand.Int(rand.Reader, &maxBig)
		return int32(output.Int64())
	}

	// Generates a random number to serve as the ticket for this Call.
	tag := genTag()

	r.tagMapLock.Lock()
	defer r.tagMapLock.Unlock()
	for {
		if _, ok := r.tagMap[tag]; ok {
			if tag < int32(1<<31-1) {
				tag++
				continue
			} else {
				tag = 0
				continue
			}
		} else {
			newBucket := &bucket{
				flag: REQUEST,
				data: nil,
				done: make(chan struct{}),
			}
			r.tagMap[tag] = newBucket
			return newBucket, tag
		}
	}

}
