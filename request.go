package blab

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"reflect"
	"time"
)

type bucket struct {
	done chan struct{}
	data *msg
}

// Call invokes a registered method/function, blocks while actively checking for for completion and returns an error, if any.
func (cl *Caller) Call(name string, arg interface{}, reply interface{}) (err error) {
	if cl.uplink == nil {
		return ErrClosed
	}

	bucket, tag := cl.getBucket()
	bucket.data = nil

	data, err := json.Marshal(arg)
	if err != nil {
		return err
	}

	data2, err := json.Marshal(reply)
	if err != nil {
		return err
	}

	cl.route(&msg{
		Dst: name,
		Va1: data,
		Va2: data2,
		Tag: tag,
	})

	// Remove bucket from map.
	reset_bucket := func() {
		cl.reqMapLock.Lock()
		delete(cl.reqMap, tag)
		cl.reqMapLock.Unlock()
	}

	for {
		select {
		// Once request is met, provide result and/or error to Caller.
		case <-bucket.done:
			if len(bucket.data.Va2) > 0 && reflect.ValueOf(reply).Kind() == reflect.Ptr {
				err = json.Unmarshal(bucket.data.Va2, reply)
				if err != nil && err != io.EOF {
					return
				}
			}

			switch bucket.data.Err {
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
			if cl.uplink == nil {
				reset_bucket()
				return ErrClosed
			}
			cl.route(&msg{
				Dst: name,
				Tag: tag * -1,
			})
			continue
		}
	}
	return
}

// Checks to see if this is a response to a Call we've placed.
func (s *session) intercept(req *msg) bool {
	s.reqMapLock.RLock()
	defer s.reqMapLock.RUnlock()
	if bucket, ok := s.reqMap[req.Tag]; ok {
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
func (cl *Caller) getBucket() (*bucket, int32) {

	// Generates a random number to serve as the ticket for this Call.
	tag := genTag()

	if tag <= 2 {
		tag = 3
	}

	cl.reqMapLock.Lock()
	defer cl.reqMapLock.Unlock()
	for {
		if _, ok := cl.reqMap[tag]; ok {
			if tag < int32(1<<31-1) {
				tag++
				continue
			} else {
				tag = 0
				continue
			}
		} else {
			newBucket := &bucket{
				done: make(chan struct{}, 0),
			}
			cl.reqMap[tag] = newBucket
			return newBucket, tag
		}
	}

}
