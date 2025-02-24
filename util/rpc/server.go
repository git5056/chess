package rpc

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/chess/util/log"
	uuid "github.com/go.uuid"
	"github.com/golang/protobuf/proto"
)

type Handler func(proto.Message) proto.Message

type Server struct {
	ch          chan bool
	waitGroup   *sync.WaitGroup
	listenPort  int
	handlers    map[string]Handler
	connHandler func(net.Conn, []byte)
}

func NewServer(port int) *Server {
	s := &Server{listenPort: port}
	s.handlers = make(map[string]Handler)
	s.ch = make(chan bool)
	s.waitGroup = &sync.WaitGroup{}
	return s
}

func (s *Server) HandleFunc(name string, handler Handler) {
	s.handlers[name] = handler
}

func (s *Server) Stop() {
	close(s.ch)
	s.waitGroup.Wait()
}

func (s *Server) Done() {
	s.waitGroup.Done()
}

func (s *Server) CheckStop() bool {
	select {
	case <-s.ch:
		return true

	default:
		return false
	}

	return false
}

func (s *Server) SetConnHandler(f func(net.Conn, []byte)) {
	s.connHandler = f
}

func (s *Server) Run(pass chan []byte) error {
	s.waitGroup.Add(1)

	defer s.waitGroup.Done()

	laddr, err := net.ResolveTCPAddr("tcp", ":"+strconv.Itoa(s.listenPort))
	if err != nil {
		return err
	}

	listener, err := net.ListenTCP("tcp", laddr)
	u1, _ := uuid.NewV4()
	fmt.Printf("UUIDv4: %s\n", u1)
	handshakeKey := u1.Bytes()
	if err != nil {
		return err
	} else {
		pass <- handshakeKey
	}

	for {
		select {
		case <-s.ch:
			listener.Close()
			return nil
		default:
		}

		listener.SetDeadline(time.Now().Add(time.Second * 3))
		conn, err := listener.AcceptTCP()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			}

			log.Error("accept fail:%s", err.Error())
			continue
		}

		s.waitGroup.Add(1)
		if s.connHandler != nil {
			go s.connHandler(conn, handshakeKey)
		} else {
			go s.handleConn(conn, handshakeKey)
		}

	}
}

func (s *Server) handleConn(conn net.Conn, handshakeKey []byte) {
	log.Info("new connection from %s", conn.RemoteAddr().String())
	br := bufio.NewReader(conn)
	bw := bufio.NewWriter(conn)

	defer conn.Close()
	defer s.waitGroup.Done()

	for {
		select {
		case <-s.ch:
			return
		default:
		}

		conn.SetDeadline(time.Now().Add(time.Second * 3))
		req, err := DecodePb(br)
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			}

			log.Error("connection from %s error: %s", conn.RemoteAddr().String(), err.Error())
			return
		}

		name := proto.MessageName(req)

		f, ok := s.handlers[name]
		if !ok {
			log.Warn("%s can not find handler", name)
			continue
		}

		resp := f(req)
		if resp == nil {
			continue
		}

		if err := EncodePb(bw, resp); err != nil {
			log.Warn("Encode Pb fail")
			return
		}

		if err := bw.Flush(); err != nil {
			log.Error("flush fail:%s", err.Error())
			return
		}
	}
}
