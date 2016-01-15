package main

import (
	"fmt"
	"gen-go/user"
	"os"
	"time"
	"zoothrift"
	"zoothrift/zk"
)

func main() {
	conn, err := zk.Connect([]string{"localhost:4180"}, time.Second*30)
	if err != nil {
		fmt.Println(err.Error())
	}
	provider := zoothrift.NewProvider(conn, "HelloService", "1.0.0")
	time.Sleep(time.Second * 1)
	zt := zoothrift.NewZooThrift(provider, &user.HelloServiceClient{})
	rs, err := zoothrift.ProxyExec(zt, "Hello", "hello")
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	if len(rs) != 0 {
		fmt.Println(rs[0].Interface().(string))
	}
}
