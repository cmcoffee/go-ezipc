package main

import (
	"fmt"
	"sync"
	"github.com/cmcoffee/go-blab"
)

type KV struct {
	mlock sync.RWMutex
	data map[string]string
}

var myKV KV

func init() {
	myKV.data = make(map[string]string)
}

func (c *KV) Set(key string, value *string) (error) {
	c.mlock.Lock()
	defer c.mlock.Unlock()
	c.data[key] = *value
	return nil
}

func (c *KV) Get(key string, output *string) (error) {
	c.mlock.RLock()
	defer c.mlock.RUnlock()
	v, ok := c.data[key]
	if !ok { return fmt.Errorf("Key not found.") }
	*output = v
	return nil
}

func countKeys(unused int, count *int) (error) {
	myKV.mlock.RLock()
	defer myKV.mlock.RUnlock()
	for _ = range myKV.data { *count++ }
	return nil
}

func main() {
	cl := blab.NewCaller()
	err := cl.Register(&myKV)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	cl.RegisterName("KVCount", countKeys)

	fmt.Println("Listening for requests!")

	err = cl.Listen("/tmp/blab.sock")
	if err != blab.ErrClosed {
		fmt.Printf("Connection Error: %s\n", err)
		return
	}
}
