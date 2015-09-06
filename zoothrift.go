package zoothrift

import (
	"errors"
	"git.apache.org/thrift.git/lib/go/thrift"
	myzk "github.com/samuel/go-zookeeper/zk"
	"math/rand"
	"net"
	"reflect"
	"strings"
	"time"
	"zoothrift/zk"
)

var (
	ErrSerAddress = errors.New("zt: service address error")
)

type ZooThrift struct {
	Hosts          []string
	SessionTimeout time.Duration
	Namespace      string
	Version        string
	Service        interface{}
	nodesCache     []string
	conn           *myzk.Conn
}

func GetNewZooThrift(hosts []string, sessionTimeout time.Duration, namespace, version string, service interface{}) *ZooThrift {
	if version == "" {
		version = "1.0.0"
	}
	return &ZooThrift{Hosts: hosts, SessionTimeout: sessionTimeout, Namespace: namespace, Version: version, Service: service}
}

func refreshNodesCache(zt *ZooThrift) error {
	if zt.conn == nil {
		conn, err := zk.Connect(zt.Hosts, zt.SessionTimeout)
		if err != nil {
			return err
		}
		zt.conn = conn
	}

	nodes, _, err := zk.GetNodesW(zt.conn, "/rpc/"+zt.Namespace+"/"+zt.Version)
	if err != nil {
		return err
	}
	zt.nodesCache = nodes
	return nil
}

func (zt *ZooThrift) GetZtClient() (interface{}, error) {
	err := refreshNodesCache(zt)
	if err != nil {
		return nil, err
	}
	if len(zt.nodesCache) == 0 {
		return nil, zk.ErrNoChild
	}

	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	ip, port := getServiceIpPort(zt.nodesCache)
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
		return nil, ErrSerAddress
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
