package main

import (
	"fmt"
	// "os"
	"time"
	"zoothrift"
	"zoothrift/examples/gen-go/user"
	"zoothrift/zk"
)

func main() {
	conn, err := zk.Connect([]string{"localhost:4180"}, time.Second*30)
	if err != nil {
		fmt.Println(err.Error())
	}
	provider := zoothrift.NewProvider(conn, "HelloService", "1.0.0")
	time.Sleep(time.Second * 1)
	client := &user.HelloServiceClient{}
	zt := zoothrift.NewZooThrift(provider, client, 20)
	for i := 0; i < 5; i++ {
		go func() {
			rs, err := zoothrift.ProxyExec(zt, "Bye", "hello")
			if err != nil {
				fmt.Println(err)
			}
			if len(rs) != 0 {
				fmt.Println(rs[0].Interface().(string), i)
			}
		}()
	}
	time.Sleep(time.Second * 5)
}
