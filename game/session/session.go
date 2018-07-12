package session

import (
	"bufio"
	"bytes"
	"net"
	"sync"
	"time"

	"github.com/chess/pb/center"
	"github.com/chess/util/log"
	"github.com/chess/util/rpc"
	"github.com/golang/protobuf/proto"
)

type Session struct {
	Gateid uint32
	Connid uint32
}

var sessionMu sync.RWMutex
var sessions map[uint32]Session = make(map[uint32]Session)
var startChan chan bool = make(chan bool)

func Init(centerHostAndPort []string, args ...interface{}) {
	go centerClient(centerHostAndPort, args...)
}

func CheckStart() {
	<-startChan
}

var HandShakeErr = CenterClientError{
	msg: "handShakeErr",
}

var ImviliadService = CenterClientError{
	msg: "imviliadService",
}

type CenterClientError struct {
	msg string
}

func (e *CenterClientError) Error() string {
	return e.msg
}

func centerClient(hostAndPort []string, args ...interface{}) {
	var conn net.Conn
	var err error
	var closeChan bool
	var currentHost string
	if len(hostAndPort) < 1 {
		return
	}
CREATE_CONN:
	printLog := true
	for {
		for _, hostPort := range hostAndPort {
			if len(currentHost) != 0 {
				hostPort = currentHost
			}
			conn, err = net.Dial("tcp", hostPort)
			if err != nil {
				if printLog {
					log.Error("connect to center server fail: %s", err.Error())
					printLog = false
				}

				time.Sleep(time.Second * 1)
				continue
			} else if len(args) > 0 {

				// handshake
				if _, err := conn.Write(args[0].([]byte)); err != nil {
					continue
				}
				respData := make([]byte, 2, 2)
				if respLen, err := conn.Read(respData); err != nil || respLen != 2 || string(respData) != "ok" {
					continue
				} else {
					currentHost = hostPort
				}
			}

			if err = sendGetAllConnInfoReq(conn); err != nil {
				log.Error("sendGetAllConnInfoReq fail: %s", err.Error())
				conn.Close()
				time.Sleep(time.Second * 1)
				continue
			}

			// break
			goto part2
		}
	}

part2:
	log.Info("connect to center server success")

	br := bufio.NewReader(conn)
	for {
		pbMsg, err := rpc.DecodePb(br)
		if err != nil {
			log.Error("recieve notify fail: %s", err.Error())
			conn.Close()
			goto CREATE_CONN
		}

		name := proto.MessageName(pbMsg)

		if name != "center.GetAllConnInfoResp" {
			log.Info("receive %s: %s", name, pbMsg.String())
		} else {
			log.Info("receive %s", name)
		}

		switch name {
		case "center.GetAllConnInfoResp":
			processGetAllConnInfoResp(pbMsg.(*center.GetAllConnInfoResp))
			if !closeChan {
				close(startChan)
				closeChan = true
			}

		case "center.NewConnInfoNotify":
			processNewConnInfoNotify(pbMsg.(*center.NewConnInfoNotify))
		case "center.DelConnInfoNotify":
			processDelConnInfoNotify(pbMsg.(*center.DelConnInfoNotify))
		case "center.DelConnInfoByGateidNotify":
			processDelConnInfoByGateidNotify(pbMsg.(*center.DelConnInfoByGateidNotify))
		}
	}
}

func CenterClient(hostAndPort string, args ...interface{}) error {
	var conn net.Conn
	var err error
	var closeChan bool

CREATE_CONN:
	printLog := true
	for {
		conn, err = net.Dial("tcp", hostAndPort)
		if err != nil {
			if printLog {
				log.Error("connect to center server fail: %s", err.Error())
				printLog = false
			}
			return err
		} else if len(args) > 0 {

			// handshake
			if _, err := conn.Write(args[0].([]byte)); err != nil {
				return err
			}
			respData := make([]byte, len(args[0].([]byte)), len(args[0].([]byte)))
			respLen, err := conn.Read(respData)
			if err != nil {
				return err
			}
			if respLen != 2 && respLen != len(args[0].([]byte)) {
				return &HandShakeErr
			}
			if respLen == 2 && string(respData[:2]) != "ok" {
				return &HandShakeErr
			}
			if respLen == len(args[0].([]byte)) && bytes.Compare(respData, args[0].([]byte)) == 0 {
				return &ImviliadService
			}
		}

		if err = sendGetAllConnInfoReq(conn); err != nil {
			log.Error("sendGetAllConnInfoReq fail: %s", err.Error())
			conn.Close()
			time.Sleep(time.Second * 1)
			continue
		}

		break
	}

	log.Info("connect to center server success")

	br := bufio.NewReader(conn)
	for {
		pbMsg, err := rpc.DecodePb(br)
		if err != nil {
			log.Error("recieve notify fail: %s", err.Error())
			conn.Close()
			goto CREATE_CONN
		}

		name := proto.MessageName(pbMsg)

		if name != "center.GetAllConnInfoResp" {
			log.Info("receive %s: %s", name, pbMsg.String())
		} else {
			log.Info("receive %s", name)
		}

		switch name {
		case "center.GetAllConnInfoResp":
			processGetAllConnInfoResp(pbMsg.(*center.GetAllConnInfoResp))
			if !closeChan {
				close(startChan)
				closeChan = true
			}

		case "center.NewConnInfoNotify":
			processNewConnInfoNotify(pbMsg.(*center.NewConnInfoNotify))
		case "center.DelConnInfoNotify":
			processDelConnInfoNotify(pbMsg.(*center.DelConnInfoNotify))
		case "center.DelConnInfoByGateidNotify":
			processDelConnInfoByGateidNotify(pbMsg.(*center.DelConnInfoByGateidNotify))
		}
	}
}

func sendGetAllConnInfoReq(conn net.Conn) error {
	var req center.GetAllConnInfoReq
	return rpc.EncodePb(conn, &req)
}

func processGetAllConnInfoResp(resp *center.GetAllConnInfoResp) {
	sessionMu.Lock()
	sessions = make(map[uint32]Session)
	for i := 0; i < len(resp.Infos); i++ {
		info := resp.Infos[i]
		sessions[info.Userid] = Session{Gateid: info.Gateid, Connid: info.Connid}
	}
	sessionMu.Unlock()
}

func processNewConnInfoNotify(notify *center.NewConnInfoNotify) {
	sessionMu.Lock()
	sessions[notify.Info.Userid] = Session{Gateid: notify.Info.Gateid, Connid: notify.Info.Connid}
	sessionMu.Unlock()
}

func processDelConnInfoNotify(notify *center.DelConnInfoNotify) {
	sessionMu.Lock()
	defer sessionMu.Unlock()

	info, present := sessions[notify.Info.Userid]
	if !present {
		return
	}

	if info.Gateid == notify.Info.Gateid && info.Connid == notify.Info.Connid {
		delete(sessions, notify.Info.Userid)
	}
}

func processDelConnInfoByGateidNotify(notify *center.DelConnInfoByGateidNotify) {
	sessionMu.Lock()
	defer sessionMu.Unlock()

	for userid, sess := range sessions {
		if sess.Gateid == notify.Gateid {
			delete(sessions, userid)
		}
	}
}

func Exist(userid uint32) bool {
	sessionMu.RLock()
	_, present := sessions[userid]
	sessionMu.RUnlock()

	return present
}

func Get(userid uint32) (Session, bool) {
	sessionMu.RLock()
	sess, present := sessions[userid]
	sessionMu.RUnlock()

	return sess, present
}

func Add(userid uint32, gateid uint32, connid uint32) {
	sessionMu.Lock()
	sessions[userid] = Session{Gateid: gateid, Connid: connid}
	sessionMu.Unlock()
}
