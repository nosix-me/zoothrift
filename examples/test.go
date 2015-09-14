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

	client, err := zt.GetZtClient()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(0)
	}
	exec := client.(*user.EchoServiceClient)
	for i := 0; i < 100; i++ {
		result, err := exec.Echo(&user.User{Age: 30, Name: "test"})
		if err != nil {
			client, _ = zt.GetZtClient()
			if client != nil {
				exec = client.(*user.EchoServiceClient)
			}
		}
		fmt.Println(result)
		time.Sleep(time.Second)
	}
}
