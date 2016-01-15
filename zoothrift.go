package zoothrift

import (
	"errors"
	"git.apache.org/thrift.git/lib/go/thrift"
	"reflect"
	"time"
)

var (
	ErrSerAddress       = errors.New("zt: service address error")
	waitNodeDelaySecond = time.Second * 1
	waitNodeDelay       = 1
	ErrMethodNotExists  = errors.New("zt: method not exists")
	ErrProxyExec        = errors.New("zt: params error")
	ErrEmptyHosts       = errors.New("zt: empty hosts")
)

type ZooThrift struct {
	Service  interface{}
	provider *Provider
}

// init a zoothrift
func NewZooThrift(provider *Provider, Service interface{}) *ZooThrift {
	zooThrift := &ZooThrift{provider: provider, Service: Service}
	return zooThrift
}

// get one client instance
func (zt *ZooThrift) GetZtClient() (interface{}, error) {
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
