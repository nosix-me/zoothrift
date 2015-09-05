package zoothrift

import (
	"gen-go/user"
	"log"
	"testing"
	"zoothrift"
)

func TestZooThrift(t *testing.T) {

	zt := zoothrift.GetNewZooThrift("127.0.0.1:4180", 3000, "test", "1.0.0", &user.EchoSericeClient{})
	// zt := zoothrift.ZooThrift{Hosts: "127.0.0.1:4180", SessionTimeout: 3000, Namespace: "3000", Version: "1.0.0", Service: &user.EchoSericeClient{}}

	client, err := zt.GetZkclient()
	if err != nil {
		log.Println(err.Error())
	}
	exec := client.(*user.EchoSericeClient)
	log.Println(exec.Echo(&user.User{Age: 20, Name: "test"}))
}
