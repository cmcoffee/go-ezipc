package main

import (
	"fmt"
	"github.com/cmcoffee/go-blab"
	"time"
)

func main() {
	cl := blab.New()
	cl.Debug = true

	err := cl.Dial("/tmp/blab.sock")
	if err != nil {
		fmt.Println(err)
		return
	}

	start_time := time.Now()

	fmt.Printf("Setting 10000 keys.\n")
	var ( 
		n int
		i int
	)
	for i = 0; i < 10000; i++ {
		n = n + i
		err = cl.Call("KV.Set", fmt.Sprintf("%d", i), fmt.Sprintf("%d", n))
		if err != nil {
			fmt.Printf("Call failed: %s\n", err)
			return
		}
	}
	fmt.Printf("Average time taken per request: %v.\n", time.Duration(float64(int64(time.Now().Sub(start_time)) / int64(i))));

	var x int

	err = cl.Call("KVCount", nil, &x)
	if err != nil {
		fmt.Println(err)
	}

	var str string
	for i := 0; i < 9; i++ {
		err = cl.Call("KV.Get", fmt.Sprintf("%d", i), &str)
		if err != nil {
			fmt.Printf("Call failed: %s\n", err)
			return
		}
		fmt.Printf("\nKey: %d, Value: %s\n", i, str)
	}

	err = cl.Call("KV.Get", fmt.Sprintf("%d", x - 1), &str)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("\nKey: %d, Value: %s\n", x - 1, str)

	fmt.Printf("\nTotal Keys: %d\n", x)
}
