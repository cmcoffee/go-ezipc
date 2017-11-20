package blab

import (
	"crypto/rand"
	"encoding/json"
	"encoding/base64"
	"errors"
	"io"
	"math/big"
	"reflect"
	"time"
	"runtime"
	"sync/atomic"
)

type bucket struct {
	done chan struct{}
	data *msg
}

var ErrBadTag = errors.New("Duplicate tag detected.")

// Call invokes a registered method/function, blocks while actively checking for for completion, returns err on failure.
func (r *router) Call(name string, arg interface{}, reply interface{}) (err error) {
	for atomic.LoadUint32(&r.ready) == 0 {
		runtime.Gosched()
	}

	data, err := json.Marshal(arg)
	if err != nil {
		return err
	}

	data2, err := json.Marshal(reply)
	if err != nil {
		return err
	}

	new_request:
	if r.uplink == nil {
		return ErrClosed
	}

	bucket, tag := r.getBucket()
	bucket.data = nil

	err = r.route(&msg{
		Dst: name,
		Va1: base64.StdEncoding.EncodeToString(data),
		Va2: base64.StdEncoding.EncodeToString(data2),
		Tag: tag,
	})
	if err != nil { return err }

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
			c := r.find_route(name)
			if c == nil && r.uplink == nil {
				reset_bucket()
				return ErrClosed
			} else {
				r.route(&msg{
					Dst: name,
					Tag: tag * -1,
				})
			}
			continue
		}
	}
	return
}

// Checks to see if this is a response to a Call we've placed.
func (s *router) intercept(req *msg) bool {
	s.tagMapLock.RLock()
	defer s.tagMapLock.RUnlock()
	if res, ok := s.tagMap[req.Tag]; ok {
		bucket := res.(*bucket)
		bucket.data = req
		bucket.done <- struct{}{}
		return true
	}
	return false
}

// Creates a random 32bit tag for IPC calls.
func genTag() int32 {
	maxBig := *big.NewInt(int64(1<<31 - 1))
	output, _ := rand.Int(rand.Reader, &maxBig)
	return int32(output.Int64())
}

// Assigned Call a bucket to capture reply with.
func (r *router) getBucket() (*bucket, int32) {

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
				data: nil,
				done: make(chan struct{}),
			}
			r.tagMap[tag] = newBucket
			return newBucket, tag
		}
	}

}
