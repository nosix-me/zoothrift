package zoothrift

import (
	log "code.google.com/p/log4go"
	"errors"
	"git.apache.org/thrift.git/lib/go/thrift"
	myzk "github.com/samuel/go-zookeeper/zk"
	"math/rand"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"zoothrift/zk"
)

var (
	ErrSerAddress       = errors.New("zt: service address error")
	ErrMethodNotExists  = errors.New("zt: method not exists")
	ErrProxyExec        = errors.New("zt: params error")
	ErrEmptyHosts       = errors.New("zt: empty hosts")
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

// init a zoothrift
func NewZooThrift(hosts []string, sessionTimeout time.Duration, namespace, version string, service interface{}) *ZooThrift {
	if version == "" {
		version = "1.0.0"
	}
	zooThrift := &ZooThrift{Hosts: hosts, SessionTimeout: sessionTimeout, Namespace: namespace, Version: version, Service: service}
	zooThrift.connect()
	go refreshNodesCache(zooThrift)
	return zooThrift
}

// connect to the zookeeper
func (zt *ZooThrift) connect() error {
	conn, err := zk.Connect(zt.Hosts, zt.SessionTimeout)
	if err != nil {
		return err
	}
	zt.conn = conn
	zk.RegisterTemp(conn, "/rpc/"+zt.Namespace, []byte{'1'})
	return nil
}

//refresh the nodes cache after received events
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
		zt.lock.Lock()
		zt.nodesCache = nodes
		zt.lock.Unlock()
		event := <-watch
		log.Info("zk path: \"%s\" receive a event %v", fpath, event)
	}
}

// get one client instance
func (zt *ZooThrift) GetZtClient() (interface{}, error) {
	time.Sleep(time.Millisecond * 10)
	for len(zt.nodesCache) == 0 {
		return nil, ErrEmptyHosts
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

//random service address
func getServiceIpPort(addresses []string) (ip, port string) {
	for _, v := range addresses {
		item := strings.Split(v, ":")
		if len(item) == 3 {
			size, err := strconv.Atoi(item[2])
			if err != nil {
				continue
			} else {
				for i := size - 1; i > 0; i-- {
					addresses = append(addresses, v)
				}
			}
		}
	}
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

// proxy exec method
func ProxyExec(zt *ZooThrift, method string, params ...interface{}) ([]reflect.Value, error) {
	if zt == nil || method == "" {
		return nil, ErrProxyExec
	}
	for {
		client, err := zt.GetZtClient()
		if err != nil {
			return nil, err
		}
		proxy := reflect.ValueOf(client)
		exec := proxy.MethodByName(method)
		if !exec.IsValid() {
			return nil, ErrMethodNotExists
		}
		param := make([]reflect.Value, len(params))
		for i, item := range params {
			param[i] = reflect.ValueOf(item)
		}
		return exec.Call(param), nil
	}
}
