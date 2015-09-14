package zoothrift

import (
	log "code.google.com/p/log4go"
	"errors"
	"git.apache.org/thrift.git/lib/go/thrift"
	myzk "github.com/samuel/go-zookeeper/zk"
	"math/rand"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"
	"zoothrift/zk"
)

var (
	ErrSerAddress       = errors.New("zt: service address error")
	waitNodeDelaySecond = time.Second * 1
	waitNodeDelay       = 1
)

type ZooThrift struct {
	Hosts          []string
	SessionTimeout time.Duration
	Namespace      string
	Version        string
	Service        interface{}
	nodesCache     []string
	conn           *myzk.Conn
	lock           sync.Mutex
}

func GetNewZooThrift(hosts []string, sessionTimeout time.Duration, namespace, version string, service interface{}) *ZooThrift {
	if version == "" {
		version = "1.0.0"
	}
	zooThrift := &ZooThrift{Hosts: hosts, SessionTimeout: sessionTimeout, Namespace: namespace, Version: version, Service: service}
	zooThrift.connect()
	go refreshNodesCache(zooThrift)
	time.Sleep(time.Second)
	return zooThrift
}

func (zt *ZooThrift) connect() error {
	conn, err := zk.Connect(zt.Hosts, zt.SessionTimeout)
	if err != nil {
		return err
	}
	zt.conn = conn
	zk.RegisterTemp(conn, "/rpc/"+zt.Namespace, []byte{'1'})
	return nil
}
func refreshNodesCache(zt *ZooThrift) {
	for {
		if zt.conn == nil {
			zt.connect()
		}
		fpath := "/rpc/" + zt.Namespace + "/" + zt.Version
		nodes, watch, err := zk.GetNodesW(zt.conn, fpath)
		if err == zk.ErrNodeNotExist {
			log.Warn("zk don't have node \"%s\", retry in %d second", fpath, waitNodeDelay)
			time.Sleep(waitNodeDelaySecond)
			continue
		} else if err == zk.ErrNoChild {
			log.Warn("zk don't have any children in \"%s\", retry in %d second", fpath, waitNodeDelay)
			time.Sleep(waitNodeDelaySecond)
			continue
		} else if err != nil {
			log.Error("getNodes error(%v), retry in %d second", err, waitNodeDelay)
			time.Sleep(waitNodeDelaySecond)
			continue
		}
		zt.nodesCache = nodes
		event := <-watch
		log.Info("zk path: \"%s\" receive a event %v", fpath, event)
	}
}

func (zt *ZooThrift) GetZtClient() (interface{}, error) {
	for len(zt.nodesCache) == 0 {
		time.Sleep(time.Second)
	}
	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	zt.lock.Lock()
	ip, port := getServiceIpPort(zt.nodesCache)
	zt.lock.Unlock()
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
