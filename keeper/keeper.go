package keeper

import (
	"errors"
	"io"
	"net"
	"time"
	"fmt"
	"sync"
	"encoding/binary"

	"github.com/glerchundi/parkeeper/backends"
	"github.com/glerchundi/parkeeper/log"

	"gopkg.in/tomb.v2"
)

const (
	bufferSize = 1536 * 1024
)

var bufferPool = sync.Pool {
	New: func() interface{} { return make([]byte, bufferSize) },
}

type Keeper struct {
	conn             net.Conn
	storeClient      backends.Client
	temp             []byte

	tomb             *tomb.Tomb

	recvChan         chan []byte
	sendChan         chan Rep
	processorChan    chan func()error
}

//
// PUBLIC
//

func NewKeeper(conn net.Conn, c backends.Client) *Keeper {
	return &Keeper {
		conn:             conn,
		storeClient:      c,
		temp:             make([]byte, 4),

		tomb:             new(tomb.Tomb),

		recvChan:         make(chan []byte, 16),
		sendChan:         make(chan Rep, 16),
		processorChan:    make(chan func()error, 16),
	}
}

func (k *Keeper) Handle()error {
	// always close connection
	defer k.Close()

	// launch frame receiver/sender
	k.trackedLoop(k.recvLoop)
	k.trackedLoop(k.sendLoop)

	// loop until connect request was received or a timeout reached
	timeout := time.After(30 * time.Second)
	select {
	case buf := <-k.recvChan:
		// try parsing connect request
		req := &ConnectReq {}
		_, err := DecodePacket(buf, req)
		if (err != nil) {
			k.tomb.Kill(err)
		}

		// create connection reply
		rep := &ConnectRep {
			ProtocolVersion: req.ProtocolVersion,
			TimeOut: req.TimeOut,
			SessionId: 1,
			Passwd: req.Passwd,
		}

		// write connection reply
		k.sendChan <- rep

		// start loops
		k.trackedLoop(k.requestLoop)
		k.trackedLoop(k.processorLoop)
	case <-k.tomb.Dead():
	case <-timeout:
		k.tomb.Kill(errors.New(fmt.Sprintf("connect wasn't received in %d", timeout)))
	}

	// main loop
	for {
		select {
		case <-k.tomb.Dying():
			// causes recv loop to EOF/exit
			k.conn.Close()
		case <-k.tomb.Dead():
			return nil
		}
	}

	return nil
}

func (k *Keeper) IsClosed() bool {
	/*
	c := k.conn
	c.SetReadDeadline(time.Now())
	if _, err := c.Read(k.temp[:1]); err == io.xEOF {
		log.Debug(fmt.Sprintf("%s detected closed LAN connection"))
	  	c.Close()
		c = nil
	} else {
		c.SetReadDeadline(time.Time{})
	}
    */
	return false
}

func (k *Keeper) Close() {
	k.conn.Close()
	k.tomb.Kill(nil)
	err := k.tomb.Wait()
	if (err != nil) {
		log.Debug("closed due to: ", err)
	}
}

//
// PRIVATE
//

func (k *Keeper) read(buf []byte) (n int, err error) {
	n, err = io.ReadFull(k.conn, buf)
	if (err != nil) {
		if (err == io.EOF || err == io.ErrUnexpectedEOF) {
		}

		return 0, err
	}

	return
}

func (k *Keeper) alloc() []byte {
	return bufferPool.Get().([]byte)
}

func (k *Keeper) free(buf []byte) {
	if (buf != nil) {
		bufferPool.Put(buf)
	}
}

func (k *Keeper) trackedLoop(f func(*tomb.Tomb)error) {
	k.tomb.Go(func() error { return f(k.tomb) })
}

func (k *Keeper) recvLoop(t *tomb.Tomb) error {
	for {
		_, err := k.read(k.temp[:4])
		if (err != nil) {
			return err
		}

		// parse frame size
		len := binary.BigEndian.Uint32(k.temp[:4])
		if (len > bufferSize) {
			return errors.New(fmt.Sprintf("length should be at most %d bytes (received %d)", bufferSize, len))
		}

		// alloc buffer (freeing if full read from conn wasn't possible)
		var buf = k.alloc()
		defer k.free(buf)

		// read frame
		_, err = k.read(buf[:len])
		if (err != nil) {
			return err
		}

		// log input
		log.Debug("<- ", fmt.Sprintf("%x%x", k.temp[:4], buf[:len]))

		// send frame to channel
		select {
		case k.recvChan <- buf[:len]:
		case <-t.Dying():
			return nil
		}
	}

	return nil
}

func (k *Keeper) sendLoop(t *tomb.Tomb) error {
	var timeout <-chan time.Time = nil
	for {
		select {
		case rep := <-k.sendChan:
			var buf []byte = k.alloc()
			defer k.free(buf)

			bytesWritten, err := EncodePacket(buf[4:], rep)
			if (err != nil) {
				return err
			}

			// write frame size
			binary.BigEndian.PutUint32(buf[:4], uint32(bytesWritten))

			// write buffer to connection
			_, err = k.conn.Write(buf[:4+bytesWritten])
			if err != nil {
				return err
			}

			// log output
			log.Debug("-> ", fmt.Sprintf("%x", buf[:4+bytesWritten]))
		case <-t.Dying():
			// create a timeout in order to send pending requests (close reply)
			if (timeout == nil) {
				timeout = time.After(100 * time.Millisecond)
			}
		case <-timeout:
			close(k.sendChan)
			return nil
		}
	}
}

func (k *Keeper) requestLoop(t *tomb.Tomb) error {
	for {
		select {
		case buf := <-k.recvChan:
			// current & total bytes read
			bytesRead := 0
			totalBytesRead := 0

			// parse request header
			reqHdr := &OpReqHeader{}
			bytesRead, err := DecodePacket(buf, reqHdr)
			totalBytesRead = totalBytesRead+bytesRead
			if (err != nil) {
				log.Error(fmt.Sprintf("unable to decode request header: %s", err.Error()))
				return err
			}

			// create request
			creator, found := creatorByOpCode[reqHdr.OpCode]
			if !found {
				log.Error(fmt.Sprintf("cannot create opcode: %d", reqHdr.OpCode))
				return err
			}

			// parse request
			req := creator()
			bytesRead, err = DecodePacket(buf[bytesRead:], req)
			totalBytesRead = totalBytesRead+bytesRead
			if (err != nil) {
				log.Error(fmt.Sprintf("unable to decode request: %s", err.Error()))
				return err
			}

			// queue processor
			k.processorChan <- func()error {
				processOpReq(OpReq{ Hdr: reqHdr, Req: req }, k.storeClient, k.sendChan)
				if (reqHdr.OpCode == opClose) {
					return errors.New("graceful connection close requested")
				}
				return nil
			}
		case <-t.Dying():
			close(k.recvChan)
			return nil
		}
	}
}

func (k *Keeper) processorLoop(t *tomb.Tomb) error {
	for {
		select {
		case f := <- k.processorChan:
			if err := f(); err != nil {
				return err
			}
		case <-t.Dying():
			close(k.processorChan)
			return nil
		}
	}
}
