package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/chess/codec"
	"github.com/chess/common"
	"github.com/chess/game/config"
	"github.com/chess/game/session"
	_ "github.com/chess/game_ddz/handler"
	"github.com/chess/game_ddz/user"
	"github.com/chess/util/log"
	"github.com/chess/util/redis_cli"
	"github.com/chess/util/rpc"
	"github.com/chess/util/services"
	zkCfg "github.com/chess/util/zookeeper"
	"github.com/go-zookeeper/zk"
)

const (
	listenProtMin = 9800
	listenProtMax = 9899
)

type CfgCenter struct {
	cfgZkNode  string
	prvdZkNode string
	zkConn     *zk.Conn
}

func (cc *CfgCenter) Start() {
	// cc.zkServer
}

func (cc *CfgCenter) Reload() {
	conn := cc.zkConn
	bExist, _, ech, err := conn.ExistsW("/chess/config/game/a")
	bError := false
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	var cfgStat *zk.Stat
	if bExist {
		cfgData, stat, echn, err := conn.GetW("/chess/config/game/a")
		if err != nil {
			return
		}
		cfgStat = stat
		err = common.InitConfigWithBytes(bytes.NewReader(cfgData))
		if err != nil {
			log.Error("prase config fail:%s", err.Error())
			bError = true
			goto p2
		}
		fmt.Println(common.GetConfig())
		// _, _, echn, err = conn.ChildrenW("/chess/config/game/a")
		ech = echn
	}
p2:
	if !bExist || bError {
		cfgData, _ := json.Marshal(common.GetConfig())
		if !bExist {
			_, err = conn.Create("/chess/config/game/a", cfgData, 0, zk.WorldACL(zk.PermAll))
		} else {
			_, err = conn.Set("/chess/config/game/a", cfgData, cfgStat.Version)
		}
		if err != nil {
			log.Error("run server fail:%s", err.Error())
		}
	}
	go cc.watch(ech)
}

func (cc *CfgCenter) Stop() {

}

func (cc *CfgCenter) watch(ech <-chan zk.Event) {
	event := <-ech

	fmt.Println("******watchCreataNode*************")
	fmt.Println("path:", event.Path)
	fmt.Println("type:", event.Type.String())
	fmt.Println("state:", event.State.String())
	fmt.Println("-------------------")
	go cc.Reload()
}

func main() {
	// if len(os.Args) < 2 {
	// 	fmt.Printf("Usage: %s conf_path\n", os.Args[0])
	// 	return
	// }

	// log.Info("server start, pid = %d", os.Getpid())

	if !config.Init("E:\\GO\\bin\\ServerGroup2\\server_game") {
		return
	}

	if !user.Init(common.GetUserAddr()) {
		return
	}

	if !redis_cli.Init(common.GetRedisAddr(), 500) {
		return
	}

	// change it
	key := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b,
		0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19,
		0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f}
	iv := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b,
		0x0c, 0x0d, 0x0e, 0x0f}
	codec.Init(key, iv)

	zkConn, err := zkCfg.CreateZCollection()
	if err != nil {
		log.Error("run server fail:%s", err.Error())
		return
	}
	defer zkConn.Close()

	cc := CfgCenter{
		zkConn: zkConn,
	}
	cc.Reload()
	for {
		time.Sleep(time.Second * 2)
	}
	findService()
	// rpc.Add(services.Center, common.GetCenterAddr(), 100)
	// rpc.Add(services.Table, common.GetTableAddr(), 1000)

	// session.Init(common.GetCenterAddr())
	// server.Run(common.GetListenPort())

}

func getPort(pc []int) int {
	for i := listenProtMin; i <= listenProtMax; i++ {
		var flag bool = false
		for _, p := range pc {
			if p == i {
				flag = true
			}
		}
		if !flag {
			return i
		}
	}
	return 0
}

func findService() {
	zkConn, err := zkCfg.CreateZCollection()
	if err != nil {
		log.Error("run server fail:%s", err.Error())
		return
	}
	defer zkConn.Close()
	chs, _, eventCh, _ := zkConn.ChildrenW("/chess/provider/center")
	go watchCreataNode(eventCh)
	for _, ch := range chs {
		fmt.Println(ch)

		prvdZNData, _, _ := zkConn.Get("/chess/provider/center/" + ch)
		prvdInfo := &common.ServiceCenterProviderInfo{}
		if err = json.Unmarshal(prvdZNData, prvdInfo); err != nil {
			log.Error("run server fail:%s", err.Error())
			return
		}
		dst := prvdInfo.IP[0] + ":" + strconv.Itoa(int(prvdInfo.Port))
		rpc.Add(services.Center, dst, 100)
		rpc.Add(services.Table, common.GetTableAddr(), 1000)
		session.Init(dst, prvdInfo.HandShake)
		// go server.Run(common.GetListenPort())
		break
	}
	for {

		time.Sleep(time.Second * 2)
	}
}

func watchCreataNode(ech <-chan zk.Event) {
	for {
		event := <-ech
		// fmt.Println("******watchCreataNode*************")
		fmt.Println("path:", event.Path)
		// fmt.Println("type:", event.Type.String())
		// fmt.Println("state:", event.State.String())
		// fmt.Println("-------------------")
		time.Sleep(time.Second * 2)
		// reload()
	}
}

var selectedTag = ""

func reload() {
	zkConn, err := zkCfg.CreateZCollection()
	if err != nil {
		log.Error("run server fail:%s", err.Error())
		return
	}
	defer zkConn.Close()
	chs, _, eventCh, _ := zkConn.ChildrenW("/chess/provider/center")
	go watchCreataNode(eventCh)
	for _, ch := range chs {

		fmt.Println(ch)
		break
	}
}

func reloadServer() {
	zkConn, err := zkCfg.CreateZCollection()
	if err != nil {
		log.Error("run server fail:%s", err.Error())
		return
	}
	defer zkConn.Close()
	zkCfg.SetCfg(zkConn, runServer2, getPort)
	chs, _, eventCh, _ := zkConn.ChildrenW("/chess/provider/center")
	go watchCreataNode(eventCh)
	for _, ch := range chs {

		fmt.Println(ch)
		break
	}
}

func runServer2(listenPort int) ([]byte, error) {
	return nil, nil
}
