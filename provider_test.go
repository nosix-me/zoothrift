package zoothrift

import (
	"fmt"
	"testing"
	"time"
	"zoothrift"
	"zoothrift/zk"
)

func TestProvider(t *testing.T) {
	conn, err := zk.Connect([]string{"192.168.1.66:2181"}, time.Second*30)
	if err != nil {
		fmt.Println(err.Error())
	}
	provider := zoothrift.NewProvider(conn, "RpcUserService", "1.0.0")
	time.Sleep(time.Second * 5)
	fmt.Println(provider.Selector())
}
