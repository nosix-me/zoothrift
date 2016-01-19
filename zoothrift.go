package zoothrift

import (
	"errors"
	"git.apache.org/thrift.git/lib/go/thrift"
	"reflect"
	"sync"
	"time"
	"zoothrift/queue"
)

var (
	ErrSerAddress          = errors.New("zt: service address error")
	waitNodeDelaySecond    = time.Second * 1
	waitNodeDelay          = 1
	ErrMethodNotExists     = errors.New("zt: method not exists")
	ErrProxyExec           = errors.New("zt: params error")
	ErrEmptyHosts          = errors.New("zt: empty hosts")
	ErrServicesUnavaliable = errors.New("zt: services unavaliable")
)

type ZooThrift struct {
	Service     interface{}
	provider    *Provider
	MaxActive   int
	activeCount int
	pool        *zoothrift.Queue
	lock        sync.Mutex
}

// init a zoothrift
func NewZooThrift(provider *Provider, Service interface{}, MaxActive int) *ZooThrift {
	zooThrift := &ZooThrift{provider: provider, Service: Service, MaxActive: MaxActive}
	zooThrift.pool = zoothrift.NewPool()
	return zooThrift
}

// get one client from pool
func (zt *ZooThrift) getClient() interface{} {
	if zt.pool.Length() == 0 {
		zt.lock.Lock()
		defer zt.lock.Unlock()
		if zt.activeCount < zt.MaxActive {
			client, err := zt.createClientFactory()
			if err != nil {
				return nil
			}
			zt.activeCount++
			return client
		}
	} else {
		return zt.pool.Peek()
	}
	count := 0
	client := zt.pool.Peek()
	for client != nil && count < 3 {
		client = zt.pool.Peek()
		time.Sleep(time.Nanosecond * 1000 * 1000 * 100)
		count++
	}
	return client
}

// return client to the pool
func (zt *ZooThrift) returnClient(client interface{}) {
	zt.pool.Add(client)
}

// create one client instance
func (zt *ZooThrift) createClientFactory() (interface{}, error) {
	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	address := zt.provider.Selector()
	if address != "" {
		transport, err := thrift.NewTSocket(address)
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

// proxy exec method
func ProxyExec(zt *ZooThrift, method string, params ...interface{}) ([]reflect.Value, error) {
	if zt == nil || method == "" {
		return nil, ErrProxyExec
	}
	client := zt.getClient()
	if client != nil {
		defer zt.returnClient(client)
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
	} else {
		return nil, ErrServicesUnavaliable
	}

}
