package main

import (
	"fmt"
	"github.com/cmcoffee/go-ezipc"
	"time"
)

func main() {
	cl := ezipc.New()

	fmt.Println("Opening socket file...")

	err := cl.Dial("/tmp/blab.sock")
	if err != nil {
		fmt.Println(err)
		return
	}

	start_time := time.Now()

	var (
		n int
		i int
	)
	for time.Since(start_time) < time.Second {
		n = n + i
		err = cl.Call("KV.Set", i, n)
		if err != nil {
			fmt.Printf("Call failed: %s\n", err)
			return
		}
		i++
	}
	fmt.Printf("\nPerformed %d requests in %v.\n", i, time.Since(start_time))

	var x int

	err = cl.Call("KVCount", nil, &x)
	if err != nil {
		fmt.Println(err)
	}

	var v int
	for i := 0; i < 9; i++ {
		err = cl.Call("KV.Get", i, &v)
		if err != nil {
			fmt.Printf("Call failed: %s\n", err)
			return
		}
		fmt.Printf("\nKey: %d, Value: %d\n", i, v)
	}

	err = cl.Call("KV.Get", x-1, &v)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("\nKey: %d, Value: %d\n", x-1, v)

	fmt.Printf("\nTotal Keys: %d\n", x)
}
