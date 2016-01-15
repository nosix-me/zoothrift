package zoothrift

import (
	"fmt"
	"testing"
	"time"
	"zoothrift"
	"zoothrift/zk"
)

func TestProvider(t *testing.T) {
	conn, err := zk.Connect([]string{"localhost:4180"}, time.Second*30)
	if err != nil {
		fmt.Println(err.Error())
	}
	provider := zoothrift.NewProvider(conn, "HelloService", "1.0.0")
	time.Sleep(time.Second)
	fmt.Println(provider.Selector())
}
