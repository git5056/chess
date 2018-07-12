package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chess/server_center/conn_info"
	"github.com/chess/server_center/handler"
	"github.com/chess/util/conf"
	"github.com/chess/util/log"
	"github.com/chess/util/rpc"
	zkCfg "github.com/chess/util/zookeeper"
	uuid "github.com/go.uuid"
	"github.com/golang/protobuf/proto"
)

var config struct {
	ListenPort int    `ini:"listen_port"`
	DataPath   string `ini:"data_path"`
}

const (
	listenProtMin = 9800
	listenProtMax = 9899
)

func initConfig(confPath string) bool {
	if err := conf.LoadIniFromFile(confPath+"/center.conf", &config); err != nil {
		log.Error("init config fail:%s", err.Error())
		return false
	}

	return true
}

var server *rpc.Server

func getListenPort() {

}

func main() {
	// if len(os.Args) < 2 {
	// 	fmt.Printf("Usage: %s conf_path\n", os.Args[0])
	// 	return
	// }

	// log.Info("server start, pid = %d", os.Getpid())

	// if !initConfig(os.Args[1]) {
	// 	return
	// }

	zkConn, err := zkCfg.CreateZCollection()
	if err != nil {
		log.Error("run server fail:%s", err.Error())
		return
	}
	defer zkConn.Close()
	// host, _ := os.Hostname()
	zkCfg.GetConfig(zkConn)
	cfgPath := zkCfg.GetCfgPath()
	fmt.Println(cfgPath)
	zkCfg.SetCfg(zkConn, runServer, getPort)
	for {
		time.Sleep(time.Second * 2)
	}

	if !conn_info.Init(config.DataPath) {
		return
	}

	server = rpc.NewServer(config.ListenPort)
	server.SetConnHandler(handleConn)

	go doSignal()

	if err := server.Run(nil); err != nil {
		log.Error("run server fail:%s", err.Error())
		return
	}

	conn_info.Close()
	log.Info("exit graceful")
}

func doSignal() {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	signal := <-ch
	log.Info("receive signal %s", signal.String())
	server.Stop()
}

func handleConn(conn net.Conn, handshakeKey []byte) {
	log.Info("new connection from %s", conn.RemoteAddr().String())

	br := bufio.NewReaderSize(conn, 1024)

	defer conn.Close()
	defer handler.RemoveClient(conn)
	defer server.Done()

	// handshake
	ticker := time.NewTicker(time.Second * 5)
	passchan := make(chan bool)
	go func() {
		buf := make([]byte, len(handshakeKey))
		if readIndex, err := conn.Read(buf); err != nil || readIndex != len(handshakeKey) || bytes.Compare(buf, handshakeKey) != 0 {
			log.Info("handshake xx")
			passchan <- false
			return
		}
		if _, err := conn.Write([]byte("ok")); err != nil {
			log.Info("handshake xx")
			passchan <- false
			return
		}
		passchan <- true
	}()
	select {
	case <-ticker.C:
		log.Info("handshake overtime")
		return
	case isOk := <-passchan:
		if !isOk {
			return
		}
		log.Info("handshake pass")
	}

	for {
		if server.CheckStop() {
			return
		}

		conn.SetDeadline(time.Now().Add(time.Second * 3))

		req, err := rpc.DecodePb(br)
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			}

			log.Error("connection from %s error: %s", conn.RemoteAddr().String(), err.Error())
			return
		}

		name := proto.MessageName(req)
		log.Info("receive request %s: %s", name, req.String())

		switch name {
		case "center.AddConnInfoReq":
			handler.HandleAddConnInfo(conn, req)
		case "center.DelConnInfoReq":
			handler.HandleDelConnInfo(conn, req)
		case "center.DelConnInfoByGateidReq":
			handler.HandleDelConnInfoByGateid(conn, req)
		case "center.GetAllConnInfoReq":
			handler.HandleGetAllConnInfo(conn, req)
		default:
			log.Info("invalid message name:%s", name)
		}
	}
}

func runServer(listenPort int) ([]byte, error) {
	config.ListenPort = listenPort
	server = rpc.NewServer(config.ListenPort)
	server.SetConnHandler(handleConn)
	passChan := make(chan []byte)
	errChan := make(chan error)
	go func() {
		if err := server.Run(passChan); err != nil {
			log.Error("run server fail:%s", err.Error())
			errChan <- err
			// return err
		}
	}()
	select {
	case handstr := <-passChan:
		log.Info("start suceessful")
		u1, _ := uuid.NewV4()
		u1.UnmarshalBinary(handstr)
		fmt.Printf("UUIDv4: %s\n", u1)
		return handstr, nil
	case err := <-errChan:
		return nil, err
	}
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
