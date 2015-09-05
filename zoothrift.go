package zoothrift

import (
	"errors"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/samuel/go-zookeeper/zk"
	"log"
	"math/rand"
	"net"
	"reflect"
	"strings"
	"time"
)

type ZooThrift struct {
	Hosts          string
	SessionTimeout time.Duration
	Namespace      string
	Version        string "1.0.0"
	Service        interface{}
	childrenCache  []string
}

func GetNewZooThrift(hosts string, sessionTimeout time.Duration, namespace, version string, service interface{}) *ZooThrift {
	if hosts == "" {
		return nil
	}
	if sessionTimeout == 0 {
		sessionTimeout = 30000
	}
	// if version == "" {
	// 	version = "1.0.0"
	// }
	return &ZooThrift{Hosts: "127.0.0.1:4180", SessionTimeout: 3000, Namespace: "test", Version: "1.0.0", Service: service}
}

func rebuild(zt *ZooThrift) error {
	if zt.Hosts == "" {
		panic("hosts can't be empty")
	}

	addresses := strings.Split(zt.Hosts, ",")
	if len(addresses) == 0 {
		return errors.New("service not found!")
	}
	c, _, err := zk.Connect(addresses, zt.SessionTimeout*1000*1000)
	if err != nil {
		return err
	}
	children, _, ch, err := c.ChildrenW("/rpc/" + zt.Namespace + "/" + zt.Version)
	if err != nil {
		return err
	}
	go EventChange(ch, zt)
	zt.childrenCache = children
	return nil
}

func (zt *ZooThrift) GetZkclient() (interface{}, error) {
	err := rebuild(zt)
	if err != nil {
		return nil, err
	}
	if len(zt.childrenCache) == 0 {
		return nil, errors.New("no service is running")
	}

	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	ip, port := getServiceIpPort(zt.childrenCache)
	if ip != "" && port != "" {
		transport, err := thrift.NewTSocket(net.JoinHostPort(ip, port))
		if err != nil {
			return nil, err
		}
		useTransport := transportFactory.GetTransport(transport)
		client := reflect.ValueOf(zt.Service)
		mutable := client.Elem()
		mutable.FieldByName("Transport").Set(reflect.Value(reflect.ValueOf(useTransport)))
		mutable.FieldByName("ProtocolFactory").Set(reflect.Value(reflect.ValueOf(protocolFactory)))
		mutable.FieldByName("InputProtocol").Set(reflect.Value(reflect.ValueOf(protocolFactory.GetProtocol(useTransport))))
		mutable.FieldByName("OutputProtocol").Set(reflect.Value(reflect.ValueOf(protocolFactory.GetProtocol(useTransport))))
		mutable.FieldByName("SeqId").SetInt(0)
		if err := transport.Open(); err != nil {
			return nil, err
		}
		return zt.Service, nil
	} else {
		return nil, errors.New("service address error")
	}
}

func EventChange(ch <-chan zk.Event, zt *ZooThrift) {
	for {
		select {
		case event := <-ch:
			if event.Type == zk.EventNodeChildrenChanged && event.Type == zk.EventNodeDataChanged {
				log.Println("children changed, rebuild cache")
				rebuild(zt)
			} else {
				log.Println("get event:", event.Type)
			}
		}
	}
}

func getServiceIpPort(addresses []string) (ip, port string) {
	index := rand.Intn(len(addresses))
	address := addresses[index]
	item := strings.Split(address, ":")
	if len(item) < 2 {
		return "", ""
	}
	ip = item[0]
	port = item[1]
	return
}
