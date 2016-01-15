package zoothrift

import (
	log "code.google.com/p/log4go"
	myzk "github.com/samuel/go-zookeeper/zk"
	"math/rand"
	"strings"
	"sync"
	"time"
	"zoothrift/zk"
)

type Provider struct {
	ZkConn    *myzk.Conn
	addes     []string
	Namespace string
	Version   string
	lock      sync.Mutex
}

func NewProvider(zkConn *myzk.Conn, Namespace, Version string) *Provider {
	if Version == "" {
		Version = "1.0.0"
	}
	provider := &Provider{ZkConn: zkConn, Namespace: Namespace, Version: Version}
	provider.buildCachePath()
	return provider
}

func (p *Provider) buildCachePath() {
	go func() {
		for {
			fpath := "/rpc/" + p.Namespace + "/" + p.Version
			nodes, watch, err := zk.GetNodesW(p.ZkConn, fpath)
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
			p.lock.Lock()
			p.addes = nodes
			p.lock.Unlock()
			event := <-watch
			log.Info("zk path: \"%s\" receive a event %v", fpath, event)
		}
	}()
}

func (p *Provider) Selector() string {
	if p.addes != nil && len(p.addes) > 0 {
		index := rand.Intn(len(p.addes))
		service := p.addes[index]
		return service[:strings.LastIndex(service, ":")]
	} else {
		return ""
	}

}
