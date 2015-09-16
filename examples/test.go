package main

import (
	"fmt"
	"gen-go/user"
	"os"
	"time"
	"zoothrift"
)

func main() {
	zt := zoothrift.NewZooThrift([]string{"localhost:4180"}, time.Second*30, "test", "1.0.0", &user.EchoServiceClient{})
	for i := 0; i < 100; i++ {
		rs, err := zoothrift.ProxyExec(zt, "Echo", &user.User{Age: 30, Name: "test"})
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
		if len(rs) != 0 {
			fmt.Println(rs[0].Interface().(string))
		}
		time.Sleep(time.Second)
	}
}
