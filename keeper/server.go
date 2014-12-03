package keeper

import (
	"net"
	"time"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/glerchundi/parkeeper/backends"
	"github.com/glerchundi/parkeeper/log"
)

type Server struct {
	addr        string
	storeClient backends.Client
	// stop gracefully without interrupting anyone
	ch          chan bool
	waitGroup   *sync.WaitGroup
}

//
// PUBLIC
//

func NewServer(addr string, storeClient backends.Client) *Server {
	server := &Server{
		addr:        addr,
		storeClient: storeClient,
		ch:          make(chan bool),
		waitGroup:   &sync.WaitGroup{},
	}
	server.waitGroup.Add(1)
	return server
}


func (s *Server) Start() {

	// Listen on 127.0.0.1:48879.  That's my favorite port number because in
	// hex 48879 is 0xBEEF.
	laddr, err := net.ResolveTCPAddr("tcp", s.addr)
	if nil != err {
		log.Fatal(err)
	}
	listener, err := net.ListenTCP("tcp", laddr)
	if nil != err {
		log.Fatal(err)
	}
	log.Debug("listening on", listener.Addr())

	// Make a new service and send it into the background.
	go s.serve(listener)

	// Handle SIGINT and SIGTERM.
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	log.Debug(<-ch)

	// Stop the service gracefully.
	s.Stop()
}

// Stop the service by closing the service's channel.  Block until the service
// is really stopped.
func (s *Server) Stop() {
	close(s.ch)
	s.waitGroup.Wait()
}

//
// PRIVATE
//

func (s *Server) serve(l *net.TCPListener) {
	defer s.waitGroup.Done()
	for {
		select {
		case <-s.ch:
			log.Debug("stopping listening on", l.Addr())
			l.Close()
			return
		default:
		}

		l.SetDeadline(time.Now().Add(1e9))
		conn, err := l.AcceptTCP()
		if nil != err {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			}
			log.Debug(err)
		}
		log.Debug(conn.RemoteAddr(), "connected")
		s.waitGroup.Add(1)

		// handle the connection in a new goroutine. This returns to listener
		// accepting code so that multiple connections may be served concurrently.
		keeper := NewKeeper(conn, s.storeClient)
		go keeper.Handle()
	}
}
