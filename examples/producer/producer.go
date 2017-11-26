package main

import (
	"fmt"
	"github.com/cmcoffee/go-ezipc"
	"sync"
)

type KV struct {
	mlock sync.RWMutex
	data  map[int]int
}

var myKV KV

func init() {
	myKV.data = make(map[int]int)
}

func (c *KV) Set(key int, value *int) error {
	c.mlock.Lock()
	defer c.mlock.Unlock()
	c.data[key] = *value
	return nil
}

func (c *KV) Get(key int, value *int) error {
	c.mlock.RLock()
	defer c.mlock.RUnlock()
	v, ok := c.data[key]
	if !ok {
		return fmt.Errorf("Key not found.")
	}
	*value = v
	return nil
}

func countKeys(unused int, count *int) error {
	myKV.mlock.RLock()
	defer myKV.mlock.RUnlock()
	for _ = range myKV.data {
		*count++
	}
	return nil
}

func Ping(unused int, unused2 *int) error {
	return nil
}

func main() {
	cl := ezipc.New()

	err := cl.Register(&myKV)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}

	cl.RegisterName("KVCount", countKeys)

	err = cl.RegisterName("Ping", Ping)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("Listening for requests!")

	err = cl.Listen("/tmp/blab.sock")
	if err != ezipc.ErrClosed {
		fmt.Printf("Connection Error: %s\n", err)
		return
	}
}
