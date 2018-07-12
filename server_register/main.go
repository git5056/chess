package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/chess/common"
	"github.com/chess/util/log"
	zkCfg "github.com/chess/util/zookeeper"
	"github.com/go-zookeeper/zk"
)

var slaveDns = map[int]map[string]interface{}{
	0: {"connectstring": "root@tcp(172.16.0.164:3306)/shiqu_tools?charset=utf8", "weight": 2},
	1: {"connectstring": "root@tcp(172.16.0.165:3306)/shiqu_tools?charset=utf8", "weight": 4},
	2: {"connectstring": "root@tcp(172.16.0.166:3306)/shiqu_tools?charset=utf8", "weight": 8},
}

var lavePrvdCenterCount = 0
var lockSlavePrvdCenter sync.RWMutex
var slavePrvdCenter = map[int]map[string]interface{}{
	// 0: {"zkpath": "/chess/provider/center", "weight": 2},
}

var i int = -1  //表示上一次选择的服务器
var cw int = 0  //表示当前调度的权值
var gcd int = 2 //当前所有权重的最大公约数 比如 2，4，8 的最大公约数为：2

func getDns() string {
	for {
		i = (i + 1) % len(slaveDns)
		if i == 0 {
			cw = cw - gcd
			if cw <= 0 {
				cw = getMaxWeight()
				if cw == 0 {
					return ""
				}
			}
		}

		if weight, _ := slaveDns[i]["weight"].(int); weight >= cw {
			return slaveDns[i]["connectstring"].(string)
		}
	}
}

func getPrvdCenter() string {
	lockSlavePrvdCenter.Lock()
	defer lockSlavePrvdCenter.Unlock()
	if len(slavePrvdCenter) < 1 {
		return ""
	}
	for {
		i = (i + 1) % len(slavePrvdCenter)
		if i == 0 {
			cw = cw - gcd
			if cw <= 0 {
				cw = getMaxWeightPrvdCenter()
				if cw == 0 {
					return ""
				}
			}
		}

		if weight, _ := slavePrvdCenter[i]["weight"].(int); weight >= cw {
			return slavePrvdCenter[i]["zkpath"].(string)
		}
	}
}

func getMaxWeightPrvdCenter() int {
	max := 0
	for _, v := range slavePrvdCenter {
		if weight, _ := v["weight"].(int); weight >= max {
			max = weight
		}
	}

	return max
}

func getMaxWeight() int {
	max := 0
	for _, v := range slaveDns {
		if weight, _ := v["weight"].(int); weight >= max {
			max = weight
		}
	}

	return max
}

func pushPrvdCenter(path string, handshake []byte) {

	center := map[string]interface{}{"zkpath": path, "handshake": handshake, "weight": 2}
	lockSlavePrvdCenter.Lock()
	slavePrvdCenter[lavePrvdCenterCount] = center
	lavePrvdCenterCount++
	lockSlavePrvdCenter.Unlock()
}

func watchZkNode(zkConn *zk.Conn, ech <-chan zk.Event) {
	event := <-ech

	fmt.Println("*******************")
	fmt.Println("path:", event.Path)
	fmt.Println("type:", event.Type.String())
	fmt.Println("state:", event.State.String())
	fmt.Println("-------------------")
	go reloadZkNode(zkConn)
}

func reloadZkNode(zkConn *zk.Conn) {
	centerPaths, _, ech, err := zkConn.ChildrenW("/chess/provider/center")
	if err != nil {
		log.Error("run server fail:%s", err.Error())
		return
	}
	var rmNodes []int
	var addNodes []int
	lockSlavePrvdCenter.RLock()
	// rm
	for k, v := range slavePrvdCenter {
		zkpath, _ := v["zkpath"].(string)
		flagExist := false
		for _, path := range centerPaths {
			if "/chess/provider/center/"+path == zkpath {
				flagExist = true
				break
			}
		}
		if !flagExist {
			rmNodes = append(rmNodes, k)
		}
	}
	// add
	for idx, path := range centerPaths {
		flagExist := false
		for _, v := range slavePrvdCenter {
			zkpath, _ := v["zkpath"].(string)
			if "/chess/provider/center/"+path == zkpath {
				flagExist = true
				break
			}
		}
		if !flagExist {
			addNodes = append(addNodes, idx)
		}
	}
	lockSlavePrvdCenter.RUnlock()

	// add action
	for _, nodeIdx := range addNodes {
		centerData, _, err := zkConn.Get("/chess/provider/center/" + centerPaths[nodeIdx])
		if err != nil {
			fmt.Println(err)
			continue
		}
		centerInfo := &common.ServiceCenterProviderInfo{}
		if err = json.Unmarshal(centerData, centerInfo); err != nil {
			fmt.Println("CC:", err, string(centerData))
			continue
		}
		pushPrvdCenter("/chess/provider/center/"+centerPaths[nodeIdx], centerInfo.HandShake)
	}

	// rm action
	var tempSlavePrvdCenter = map[int]map[string]interface{}{}
	var loopC = 0
	if len(rmNodes) > 0 {
		lockSlavePrvdCenter.Lock()
		for k, v := range slavePrvdCenter {
			flagExist := false
			for _, nodeIdx := range rmNodes {
				if nodeIdx == k {
					flagExist = true
					break
				}
			}
			if !flagExist {
				tempSlavePrvdCenter[loopC] = v
				loopC++
			} else {
				if i >= k {
					i-- // ??
				}
			}
		}
		slavePrvdCenter = tempSlavePrvdCenter
		lavePrvdCenterCount = len(slavePrvdCenter)
		lockSlavePrvdCenter.Unlock()
	}

	go watchZkNode(zkConn, ech)
}

func callback(event zk.Event) {
	fmt.Println("*******************")
	fmt.Println("path:", event.Path)
	fmt.Println("type:", event.Type.String())
	fmt.Println("state:", event.State.String())
	fmt.Println("-------------------")
}

func main() {
	// option := zk.WithEventCallback(callback)
	zkConn, err := zkCfg.CreateZCollection()
	if err != nil {
		log.Error("run server fail:%s", err.Error())
		return
	}
	defer zkConn.Close()

	centerPaths, _, ech, err := zkConn.ChildrenW("/chess/provider/center")
	if err != nil {
		log.Error("run server fail:%s", err.Error())
		return
	}
	for _, centerPath := range centerPaths {
		centerData, _, _ := zkConn.Get("/chess/provider/center/" + centerPath)
		centerInfo := &common.ServiceCenterProviderInfo{}
		if err = json.Unmarshal(centerData, centerInfo); err != nil {
			log.Error("run server fail:%s", err.Error())
			continue
		}
		pushPrvdCenter("/chess/provider/center/"+centerPath, centerInfo.HandShake)
	}
	go watchZkNode(zkConn, ech)

	// note := map[string]int{}

	// s_time := time.Now().Unix()

	// if len(slavePrvdCenter) > 0 {
	// 	for i := 0; i < 100; i++ {
	// 		s := getPrvdCenter()
	// 		fmt.Println(s)
	// 		if note[s] != 0 {
	// 			note[s]++
	// 		} else {
	// 			note[s] = 1
	// 		}
	// 	}

	// 	e_time := time.Now().Unix()

	// 	fmt.Println("total time: ", e_time-s_time)

	// 	fmt.Println("--------------------------------------------------")

	// 	for k, v := range note {
	// 		fmt.Println(k, " ", v)
	// 	}
	// }

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if len(slavePrvdCenter) > 0 {
			s := getPrvdCenter()
			fmt.Println(s)
			fmt.Fprint(w, s)
		} else {
			w.WriteHeader(404)
		}
	})
	log.Error("", http.ListenAndServe(":9080", nil))

}
