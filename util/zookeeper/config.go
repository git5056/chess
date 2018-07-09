package zookeeper

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
)

const zkServer = "localhost:2181"
const (
	PathAppRoot       = "/chess"
	PathConfig        = "/chess/config"
	PathCfgServer     = "/chess/config/center"
	PathCfgServerLock = "/chess/config/center/lock"
)

var zkConn zk.Conn
var acls = zk.WorldACL(zk.PermAll)
var lockPath = ""
var ExecPath = ""

func init() {
	ExecPath = os.Args[0]
}

func Init() {
	zkConn, err := CreateZCollection()
	if err != nil {

	}
	if zkConn != nil {

	}
}

func CreateZCollection() (*zk.Conn, error) {
	var hosts = []string{zkServer}
	conn, _, err := zk.Connect(hosts, time.Second*5)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func CreateZNode(conn *zk.Conn, path string, flag int32, data []byte) bool {
	_, err_create := conn.Create(path, data, flag, acls)
	if err_create != nil {
		fmt.Println(err_create)
		return false
	}
	return true
}

func createEmptyNode(conn *zk.Conn, path string, flag int32) (bool, error) {
	_, err_create := conn.Create(path, []byte(""), flag, acls)
	if err_create != nil {
		fmt.Println(err_create)
		return false, err_create
	}
	return true, nil
}

func CreateEmptyNodeW(conn *zk.Conn, path string, flag int32) (bool, error) {
	splitPath := strings.Split(strings.TrimLeft(path, "/"), "/")
	path = ""
	loopC := len(splitPath)
	for idx, p := range splitPath {
		var _flag int32 = 0
		if idx == loopC-1 {
			_flag = flag
		}
		path = path + "/" + p
		_, err := conn.Create(path, []byte(""), _flag, acls)
		if err != nil {
			if err.Error() != "zk: node already exists" {
				fmt.Println(err)
				return false, err
			} else if idx == loopC-1 {
				return false, err
			}
		}
	}
	return true, nil
}

func SaveZNode(conn *zk.Conn, path string, flag int32, data []byte, version int32) error {
	bExist, _, _ := conn.Exists(path)
	if bExist {
		if _, err := conn.Set(path, data, version); err != nil {
			return err
		}
	} else {
		_, err := CreateEmptyNodeW(conn, path, flag)
		if err != nil && err.Error() == "zk: node already exists" {
			return err
		}
		if _, err := conn.Set(path, data, version); err != nil {
			return err
		}
	}
	return nil
}

func SetLock(conn *zk.Conn, path string) (bool, error) {
	_, err := conn.Create(path, []byte(""), zk.FlagEphemeral, acls)
	if err != nil {
		if err.Error() == "zk: node already exists" {
			return false, nil
		} else {
			return false, err
		}
	}
	return true, nil
}

func nop() {}

func GetConfig(conn *zk.Conn) {
	if _, err := CreateEmptyNodeW(conn, PathCfgServerLock, 0); err != nil && err.Error() != "zk: node already exists" {
		log.Panic(err)
	}

	loopC := 0
	for {
		nodeLock := PathCfgServerLock + "/" + ExecPath + "_" + strconv.Itoa(loopC)
		fmt.Println(nodeLock)
		isLocked, _ := SetLock(conn, nodeLock)
		if isLocked {
			lockPath = nodeLock
			//SetCfg(conn)
			break
		} else {
			nop()
			nop()
		}
		loopC++
		time.Sleep(time.Second)
	}

	// data, _, err := conn.Get(pathCfgServer + "/" + execPath)
	// if err != nil {
	// 	log.Println("aa", err.Error())
	// }
	// dataStr := string(data)
	// fmt.Println(dataStr)
}

func SetCfg(conn *zk.Conn, deal func(int) (string, error), getPort func(except []int) int) {
	cfgPath := GetCfgPath()
	fmt.Println(cfgPath)
	var except []int
	var exceptC = 0
	host, _ := os.Hostname()
	pathListenPort := cfgPath + "/listen_port"

	bExist, _, _ := conn.Exists(pathListenPort)
	if bExist {
		data, _, _ := conn.Get(pathListenPort)
		if len(data) > 0 {
			listenPort, _ := strconv.Atoi(string(data))
			pathProvider := GetProdPath(listenPort)
			_, err := CreateEmptyNodeW(conn, pathProvider, zk.FlagEphemeral)
			if err != nil {

			}
			if _, err := deal(listenPort); err != nil {
				fmt.Println(err.Error())
				except = append(except, listenPort)
				exceptC++
				err = conn.Delete(pathListenPort, 0)
				if err != nil {
					fmt.Println(err.Error())
				}
				err = conn.Delete(pathProvider, 0)
				if err != nil {
					fmt.Println(err.Error())
				}
			} else {
				fmt.Println("successful")
				return
				// break
				// zkCfg.CreateZNode(zkConn, cfgPath+"/listen_port", 0, []byte(strconv.Itoa(listenPort)))
			}
		}
	}

	for {
		childrenPaths, _, _ := conn.Children(PathCfgServer)
		nop()
		nop()
		for _, p := range childrenPaths {
			if strings.HasPrefix(p, host) {
				d, _, err := conn.Get(PathCfgServer + "/" + p + "/listen_port")
				if err == nil {
					dint, err := strconv.Atoi(string(d))
					if err == nil {
						except = append(except, dint)
					}
				}
			}
		}
		nop()
		nop()
		listenPort := getPort(except)
		pathProvider := GetProdPath(listenPort)
		_, err := CreateEmptyNodeW(conn, pathProvider, zk.FlagEphemeral)
		if err != nil {
			fmt.Println(err.Error())
		} else {
			if handstr, err := deal(listenPort); err != nil {
				except = append(except[:exceptC], listenPort)
				exceptC++
				err = conn.Delete(pathListenPort, 0)
				if err != nil {
					fmt.Println(err.Error())
				}
				err = conn.Delete(pathProvider, 0)
				if err != nil {
					fmt.Println(err.Error())
				}
			} else {
				nop()
				nop()
				fmt.Println(handstr)
				fmt.Println(pathListenPort)
				err := SaveZNode(conn, pathListenPort, 0, []byte(strconv.Itoa(listenPort)), 0)
				if err != nil {
					fmt.Println(err.Error())
				}
				break
			}
		}
		time.Sleep(time.Second)
	}
}

func GetCfgPath() string {
	cfgPath := ""
	idx := strings.LastIndex(lockPath, "_")
	num, _ := strconv.Atoi(lockPath[idx+1:])
	host, _ := os.Hostname()
	cfgPath = PathCfgServer + "/" + host + "_" + ExecPath

	if num != 0 {
		cfgPath += lockPath[idx+1:]
	}
	return cfgPath
}

func GetProdPath(port int) string {
	host, _ := os.Hostname()
	return "/chess/provider/center/" + host + "_" + strconv.Itoa(port)
}

func GetPorts() []int {

	return nil
}
