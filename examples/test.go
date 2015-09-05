package main

import (
	"fmt"
	"gen-go/user"
	"os"
	"zoothrift"
)

func main() {
	zt := zoothrift.GetNewZooThrift("127.0.0.1:4180", 3000, "test", "1.0.0", &user.EchoServiceClient{})

	client, err := zt.GetZtClient()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(0)
	}
	exec := client.(*user.EchoServiceClient)
	fmt.Println(exec.Echo(&user.User{Age: 30, Name: "test"}))
}
