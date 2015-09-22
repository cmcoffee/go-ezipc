package main

import (
	"fmt"
	"github.com/cmcoffee/go-blab"
)

func  main() {
	cl := blab.NewCaller()

	err := cl.Dial("/tmp/blab.sock")
	if err != nil {
		fmt.Printf("Err Calling Broker: %s\n", err)
		return
	}
	err = cl.Call("KV.Set", "Hello", "World")
	if err != nil {
		fmt.Printf("Call failed: %s\n", err)
		return
	}
	var str string
	err = cl.Call("KV.Get", "Hello", &str)
	if err != nil {
		fmt.Printf("Call failed: %s\n", err)
		return
	}

	var x int
	err = cl.Call("KVCount", nil, &x)

	fmt.Printf("Key: Hello, Value: %s, Total Keys in KV: %d\n", str, x)
}
