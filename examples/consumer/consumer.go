package main

import (
	"fmt"
	"github.com/cmcoffee/go-blab"
	"time"
)

func main() {
	blab.Debug = false
	cl := blab.NewCaller()

	err := cl.Dial("/tmp/blab.sock")
	if err != nil {
		fmt.Printf("Err Calling Broker: %s\n", err)
		return
	}

	fmt.Printf("Setting 10 keys.\n")
	start := time.Now()
	var n int
	for i := 0; i < 10; i++ {
		n = n + i
		err = cl.Call("KV.Set", fmt.Sprintf("%d", i), fmt.Sprintf("%d", n))
		if err != nil {
			fmt.Printf("Call failed: %s\n", err)
			return
		}
	}
	fmt.Printf("Total request time: %v.\n", time.Since(start))

	var x int
	fmt.Printf("\nGetting Key Count...\n")
	start = time.Now()
	err = cl.Call("KVCount", nil, &x)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("Request Took: %v to complete, Total Keys in keystore:%d\n", time.Since(start), x)

	var str string
	for i := 0; i < 10; i++ {
		err = cl.Call("KV.Get", fmt.Sprintf("%d", i), &str)
		if err != nil {
			fmt.Printf("Call failed: %s\n", err)
			return
		}
		fmt.Printf("\nKey: %d, Value: %s\n", i, str)
	}
}
