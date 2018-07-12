package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/chess/codec"
	"github.com/chess/common"
	"github.com/chess/game/config"
	"github.com/chess/game/server"
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
	listenProtMin = 9900
	listenProtMax = 9999
)

type CfgCenter struct {
	cfgZkNode  string
	prvdZkNode string
	zkConn     *zk.Conn
	lockPrefix string
}

var zkConnExtern *zk.Conn

func (cc *CfgCenter) Start() {
	// cc.zkServer
}

func (cc *CfgCenter) Init() {
	conn := cc.zkConn
	acl, _, _ := conn.GetACL("/chess")
	// var except []int
	// var exceptC = 0
	execPath := os.Args[0]
	host, _ := os.Hostname()
	lockPrefix := "/chess/lo2ck/" + cc.lockPrefix + "config_"
	lockPath := lockPrefix + strings.ToUpper(host+"_"+execPath)

	// if bExist, _, _ := conn.Exists(lockPrefix); !bExist {
	// 	zkCfg.CreateEmptyNodeW(conn, lockPrefix, zk.FlagEphemeral)
	// }
	var loopC int = 0
	for {
		var lp string = lockPath + "_" + strconv.Itoa(loopC)
		fmt.Println(lp)
		_, err := conn.Create(lp, []byte(""), zk.FlagEphemeral, acl)
		if err == zk.ErrNodeExists {
			loopC++
			continue
		}
		if err == zk.ErrNoNode {
			zkCfg.CreateEmptyNodeW(conn, lockPrefix, zk.FlagEphemeral)
			continue
		}
		break
	}
	for {
		time.Sleep(time.Second)
	}

}

func (cc *CfgCenter) Reload() {
	conn := cc.zkConn
	host, _ := os.Hostname()
	bExist, _, ech, err := conn.ExistsW("/chess/config/game/" + host + "_" + "1234")
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
			goto part2
		}
		fmt.Println(common.GetConfig())
		// _, _, echn, err = conn.ChildrenW("/chess/config/game/a")
		ech = echn
	}
part2:
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
	go cc.RunGame()
	go cc.watch(ech)
}

func (cc *CfgCenter) Stop() {

}

func (cc *CfgCenter) RunService() {
	conn := cc.zkConn

	for {
		resp, err := http.Get("http://127.0.0.1:9080")
		if err != nil {
			fmt.Println(err)
			continue
		}

		body, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if len(body) == 0 {
			fmt.Println("无服务可以使用")
			time.Sleep(time.Second * 2)
			continue
		} else {
			fmt.Println(string(body))
		}
		var targetService = string(body)
		bExist, _, _, err := conn.ExistsW(targetService)
		if err != nil {
			fmt.Println(err)
		}
		if bExist {
			dd := 2
			dd++
			_, err = connectService(conn, targetService)
			if err != nil {
				fmt.Println("连接服务失败")
				continue
			}
		} else {
			fmt.Println("节点不可用")
			continue
		}
	}
}

func (cc *CfgCenter) RunGame() {
	conn := cc.zkConn
	host, _ := os.Hostname()
	lockPrefix := cc.lockPrefix + host + "_"
	lockPath := "/chess/lock/" + lockPrefix + strconv.Itoa(getPort(nil))

	var ips []string
	addrs, err := net.InterfaceAddrs()

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	for _, address := range addrs {

		// 检查ip地址判断是否回环地址
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ips = append(ips, ipnet.IP.String())
			}
		}
	}
	pathProvider := "/chess/provider/game/" + host + "_" + strconv.Itoa(common.GetListenPort())
	prvdInfo := common.ServiceCenterProviderInfo{
		WorkSpace: os.Args[0],
		StartTime: time.Now().Unix(),
		IP:        ips,
		Port:      int32(common.GetListenPort()),
	}

	zkCfg.SetLock(conn, lockPath)
	for {
		err := server.Run(common.GetListenPort(), func(args ...interface{}) (interface{}, error) {
			prvdInfo.HandShake = args[2].([]byte)
			prvdZNData, _ := json.Marshal(prvdInfo)
			return zkCfg.CreateZNodeW(conn, pathProvider+"#"+time.Now().String(), zk.FlagEphemeral, prvdZNData)
		})
		if err != nil {
			fmt.Println(err)
			// 判断是具体错误并相应处理
		}
	}
	conn.Delete(lockPath, 0)
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
		zkConn:     zkConn,
		lockPrefix: "gamelock_",
	}
	cc.Init()
	go cc.RunService()
	// serviceStat()
	go cc.Reload()
	// cc.RunGame()
	for {
		time.Sleep(time.Second * 2)
	}

	// findService()
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

func findService(targetService string) (*zk.Conn, error) {
	zkConn, err := zkCfg.CreateZCollection()
	if err != nil {
		log.Error("run server fail:%s", err.Error())
		return nil, err
	}

	prvdZNData, _, ech, err := zkConn.GetW(targetService)
	if err != nil {
		log.Error("run server fail:%s", err.Error())
		return nil, err
	}
	prvdInfo := &common.ServiceCenterProviderInfo{}
	if err = json.Unmarshal(prvdZNData, prvdInfo); err != nil {
		log.Error("run server fail:%s", err.Error())
		return nil, err
	}
	var dst []string
	for _, ip := range prvdInfo.IP {
		dst = append(dst, ip+":"+strconv.Itoa(int(prvdInfo.Port)))
	}
	rpc.Add(services.Center, dst[0], 100)
	rpc.Add(services.Table, common.GetTableAddr(), 1000)
	session.Init(dst, prvdInfo.HandShake)
	go watchCreataNode(ech)
	return zkConn, nil
}

func connectService(zkConn *zk.Conn, targetService string) (*zk.Conn, error) {
	prvdZNData, _, ech, err := zkConn.GetW(targetService)
	if err != nil {
		log.Error("run server fail:%s", err.Error())
		return nil, err
	}
	prvdInfo := &common.ServiceCenterProviderInfo{}
	if err = json.Unmarshal(prvdZNData, prvdInfo); err != nil {
		log.Error("run server fail:%s", err.Error())
		return nil, err
	}

	rpc.Add(services.Table, common.GetTableAddr(), 1000)
	for _, ip := range prvdInfo.IP {
		ipAndPort := ip + ":" + strconv.Itoa(int(prvdInfo.Port))
		rpc.Add(services.Center, ipAndPort, 100)
		err := session.CenterClient(ipAndPort, prvdInfo.HandShake)
		if err != nil {
			fmt.Println(err)
		}
	}
	go watchCreataNode(ech)
	return zkConn, nil
}

func serviceStat() {
find:
	resp, err := http.Get("http://127.0.0.1:9080")
	if err != nil {
		// handle error
	}
	// defer
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if len(body) == 0 {
		fmt.Println("无服务可以使用")
		time.Sleep(time.Second * 2)
		goto find
	} else {
		fmt.Println(string(body))
		var targetService = string(body)
		zkConnExtern, _ = findService(targetService)
		// for {
		// 	var err2 error

		// 	if err2 != nil {
		// 		continue
		// 	}
		// 	break
		// }

	}
}

func pauseService() {

}

func watchCreataNode(ech <-chan zk.Event) {
	event := <-ech
	fmt.Println("*******************")
	fmt.Println("path:", event.Path)
	fmt.Println("type:", event.Type.String())
	fmt.Println("state:", event.State.String())
	fmt.Println("-------------------")
	fmt.Println("pauseService")
	// 节点丢失
	// 暂停服务
	pauseService()

	// 重新寻找服务
	time.Sleep(time.Second)
	go serviceStat()

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
