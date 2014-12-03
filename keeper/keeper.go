package keeper

import (
	"io"
	"net"
	"time"
	"fmt"
	"sync"
	"errors"
	"encoding/binary"

	"github.com/glerchundi/parkeeper/backends"
	"github.com/glerchundi/parkeeper/log"

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
	gracefullyClosed bool
	temp             []byte
}

//
// PUBLIC
//

func NewKeeper(conn net.Conn, c backends.Client) *Keeper {
	return &Keeper {
		conn:             conn,
		storeClient:      c,
		gracefullyClosed: false,
		temp:             make([]byte, 4),
	}
}

func (k *Keeper) Handle() {
	ch := make(chan error)
	f := func() {
		defer func() {
			if err := recover(); err != nil {
				ch <- errors.New("unexpected error")
			}
		}()

		err := k.processFrame(func(buf []byte) {
			req := &ConnectReq {}
			_, err := DecodePacket(buf, req)
			if (err != nil) {
				ch <- err
				return
			}

			// create connection reply
			rep := &ConnectRep {
				ProtocolVersion: req.ProtocolVersion,
				TimeOut: req.TimeOut,
				SessionId: 1,
				Passwd: req.Passwd,
			}

			// write connection reply
			k.writeRep(rep)

			// signal as successful
			ch <- nil
		})

		if (err != nil) {
			ch <- err
		}
	}

	timeout := time.After(30 * time.Second)
	for !k.IsClosed() {
		go f()
		select {
		case err := <- ch:
			if (err != nil) {
				break
			}
			k.loop()
		case <-timeout:
			k.Close()
		}
	}
}

func (k *Keeper) IsClosed() bool {
	/*
	c := k.conn
	c.SetReadDeadline(time.Now())
	if _, err := c.Read(k.temp[:1]); err == io.EOF {
		log.Debug(fmt.Sprintf("%s detected closed LAN connection"))
	  	c.Close()
		c = nil
	} else {
		c.SetReadDeadline(time.Time{})
	}
  */
	return k.gracefullyClosed
}

func (k *Keeper) Close() {
	log.Info("closing connection...")
	defer k.conn.Close()
	k.gracefullyClosed = true
}

//
// PRIVATE
//

var processorByOpCode = map[int32]func(OpReq,backends.Client)*OpRep {
	opCreate: func (opReq OpReq, c backends.Client) *OpRep { return processCreateReq(opReq, c) },
	opDelete: func (opReq OpReq, c backends.Client) *OpRep { return processDeleteReq(opReq, c) },
	opExists: func (opReq OpReq, c backends.Client) *OpRep { return processExistsReq(opReq, c) },
	opGetData: func (opReq OpReq, c backends.Client) *OpRep { return processGetDataReq(opReq, c) },
	opSetData: func (opReq OpReq, c backends.Client) *OpRep { return processSetDataReq(opReq, c) },
	opGetAcl: func (opReq OpReq, c backends.Client) *OpRep { return processGetAclReq(opReq) },
	opSetAcl: func (opReq OpReq, c backends.Client) *OpRep { return processSetAclReq(opReq) },
	opGetChildren: func (opReq OpReq, c backends.Client) *OpRep { return processGetChildrenReq(opReq, c) },
	opSync: func (opReq OpReq, c backends.Client) *OpRep { return processSyncReq(opReq) },
	opPing: func (opReq OpReq, c backends.Client) *OpRep { return processPingReq(opReq) },
	opGetChildren2: func (opReq OpReq, c backends.Client) *OpRep { return processGetChildren2Req(opReq, c) },
	opCheck: func (opReq OpReq, c backends.Client) *OpRep { return processCheckVersionReq(opReq) },
	opMulti: func (opReq OpReq, c backends.Client) *OpRep { return processMultiReq(opReq) },
	opCreate2: func (opReq OpReq, c backends.Client) *OpRep { return processCreate2Req(opReq, c) },
	opClose: func (opReq OpReq, c backends.Client) *OpRep { return processCloseReq(opReq) },
	opSetAuth: func (opReq OpReq, c backends.Client) *OpRep { return processSetAuthReq(opReq) },
	opSetWatches: func (opReq OpReq, c backends.Client) *OpRep { return processSetWatchesReq(opReq) },
}

var keeperErrFromBackendErr = map[int]int32 {
	backends.Unknown:            errSystemError,
	backends.Unimplemented:      errUnimplemented,
	backends.BackendUnreachable: errSystemError,
	backends.KeyNotFound:        errNoNode,
	backends.KeyExists:          errNodeExists,
	backends.BadVersion:         errBadVersion,
}

func (k *Keeper) read(buf []byte) (n int, err error) {
	n, err = io.ReadFull(k.conn, buf)
	if (err != nil) {
		if (err == io.EOF || err == io.ErrUnexpectedEOF) {
		}

		return 0, err
	}

	return
}

func (k *Keeper) processFrame(f func([]byte)) (err error) {
	var buf []byte = nil
	defer func() { k.free(buf) }()

	_, err = k.read(k.temp[:4])
	if (err != nil) {
		return err
	}

	// parse frame size
	len := binary.BigEndian.Uint32(k.temp[:4])

	// alloc buffer (freeing if full read from conn wasn't possible)
	buf = k.alloc()
	_, err = k.read(buf[:len])
	if (err != nil) {
		return err
	}

	// log input
	log.Debug("<-", fmt.Sprintf("%x%x", k.temp[:4], buf[:len]))

	// process frame
	f(buf[:len])

	return nil
}

func (k *Keeper) writeRep(rep interface{}) (err error) {
	var buf []byte = k.alloc()
	defer func() { k.free(buf) }()

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
	log.Debug("->", fmt.Sprintf("%x", buf[:4+bytesWritten]))

	return nil
}

func (k *Keeper) alloc() []byte {
	return bufferPool.Get().([]byte)
}

func (k *Keeper) free(buf []byte) {
	if (buf != nil) {
		bufferPool.Put(buf)
	}
}

func (k *Keeper) loop() {
	defer func() {
		if err := recover(); err != nil {
			log.Debug(fmt.Sprintf("%s", err))
		}
	}()

	for {
		err := k.processFrame(func (buf []byte) {
			// current & total bytes read
			bytesRead := 0
			totalBytesRead := 0

			// parse request header
			reqHdr := &OpReqHeader{}
			bytesRead, err := DecodePacket(buf, reqHdr)
			totalBytesRead = totalBytesRead + bytesRead
			if (err != nil) {
				log.Error(fmt.Sprintf("unable to decode request header: %s", err.Error()))
				return
			}

			// create request
			creator, found := creatorByOpCode[reqHdr.OpCode]
			if !found {
				log.Error(fmt.Sprintf("cannot create opcode: %d", reqHdr.OpCode))
				return
			}

			// parse request
			req := creator()
			bytesRead, err = DecodePacket(buf[bytesRead:], req)
			totalBytesRead = totalBytesRead + bytesRead
			if (err != nil) {
				log.Error(fmt.Sprintf("unable to decode request: %s", err.Error()))
				return
			}

			// find processor
			processor, found := processorByOpCode[reqHdr.OpCode]
			if !found {
				log.Error(fmt.Sprintf("cannot process opcode: %d", reqHdr.OpCode))
				return
			}

			// process request & write rep (if needed)
			rep := processor(OpReq{ Hdr: reqHdr, Req: req }, k.storeClient)
			if (rep != nil) {
				if err := k.writeRep(rep); err != nil {
					log.Error(fmt.Sprintf("cannot write response: %s", err.Error()))
				}
			}
		})

		if (err != nil) {
			log.Debug(err.Error())
			return
		}
	}
}

func mapBackendError(err *backends.Error) int32 {
	errCode := err.Code()
	keeperErr, found := keeperErrFromBackendErr[errCode]
	if !found {
		log.Error(fmt.Sprintf("unexpected client error: ", errCode))
		return errSystemError
	}

	return keeperErr
}

func newRep(xid int32, zxid int64, err int32, rep interface{}) *OpRep {
	return &OpRep {
		Hdr: &OpRepHeader{ Xid: xid, Zxid: zxid, Err: err },
		Rep: rep,
	}
}

func newErrorRep(xid int32, zxid int64, err int32) *OpRep {
	var empty struct{}
	return newRep(xid, zxid, err, empty)
}

func newBackendErrorRep(xid int32, zxid int64, err *backends.Error) *OpRep {
	return newErrorRep(xid, zxid, mapBackendError(err))
}

func newErrorRepIfInvalidPath(xid int32, zxid int64, path *Path) *OpRep {
	if (!path.IsValid()) {
		return newErrorRep(xid, zxid, errBadArguments)
	}
	return nil
}

func newStat(createdIndex uint64, modifiedIndex uint64, dataLength int) Stat {
	return Stat {
		CreatedZxid: int64(createdIndex),
		ModifiedZxid: int64(modifiedIndex),
		CreatedTime: 0,
		ModifiedTime: 0,
		Version: int32(modifiedIndex),
		ChildrenVersion: 0,
		AclVersion: 0,
		EphemeralOwner: 0,
		DataLength: int32(dataLength),
		NumChildren: 0,
		Pzxid: 0,
	}
}

func processCreateReq(opReq OpReq, c backends.Client) *OpRep {
	xid := opReq.Hdr.Xid
	req := opReq.Req.(*CreateReq)
	if err := newErrorRepIfInvalidPath(xid, 0, req.Path); err != nil {
		return err
	}

	err := c.Create(req.Path.Value, string(req.Data))
	if (err != nil) {
		return newBackendErrorRep(xid, 0, err)
	}

	return newRep(
		xid, 0, errOk,
		&CreateRep { Path: req.Path.Value },
	)
}

func processDeleteReq(opReq OpReq, c backends.Client) *OpRep {
	xid := opReq.Hdr.Xid
	req := opReq.Req.(*DeleteReq)
	if err := newErrorRepIfInvalidPath(xid, 0, req.Path); err != nil {
		return err
	}

	err := c.Delete(req.Path.Value, req.Version)
	if (err != nil) {
		return newBackendErrorRep(xid, 0, err)
	}

	return newRep(
		xid, 0, errOk,
		&DeleteRep {},
	)
}

func processExistsReq(opReq OpReq, c backends.Client) *OpRep {
	xid := opReq.Hdr.Xid
	req := opReq.Req.(*ExistsReq)
	if err := newErrorRepIfInvalidPath(xid, 0, req.Path); err != nil {
		return err
	}

	err := c.Exists(req.Path.Value)
	if (err != nil) {
		return newBackendErrorRep(xid, 0, err)
	}

	return newRep(
		xid, 0, errOk,
		&ExistsRep { Stat: newStat(0, 0, 0) },
	)
}

func processGetDataReq(opReq OpReq, c backends.Client) *OpRep {
	xid := opReq.Hdr.Xid
	req := opReq.Req.(*GetDataReq)
	if err := newErrorRepIfInvalidPath(xid, 0, req.Path); err != nil {
		return err
	}

	node, err := c.GetData(req.Path.Value)
	if (err != nil) {
		return newBackendErrorRep(xid, 0, err)
	}

	return newRep(
		xid, 0, errOk,
		&GetDataRep {
			Data: []byte(node.Value),
			Stat: newStat(node.CreatedIndex, node.ModifiedIndex, len(node.Value)),
		},
	)
}

func processSetDataReq(opReq OpReq, c backends.Client) *OpRep {
	xid := opReq.Hdr.Xid
	req := opReq.Req.(*SetDataReq)
	if err := newErrorRepIfInvalidPath(xid, 0, req.Path); err != nil {
		return err
	}

	err := c.SetData(req.Path.Value, string(req.Data), req.Version)
	if (err != nil) {
		return newBackendErrorRep(xid, 0, err)
	}

	return newRep(
		xid, 0, errOk,
		&SetDataRep { Stat: newStat(0, 0, 0) },
	)
}

func processGetAclReq(opReq OpReq) *OpRep {
	return newErrorRep(opReq.Hdr.Xid, 0, errUnimplemented)
}

func processSetAclReq(opReq OpReq) *OpRep {
	return newErrorRep(opReq.Hdr.Xid, 0, errUnimplemented)
}

func processGetChildrenReq(opReq OpReq, c backends.Client) *OpRep {
	xid := opReq.Hdr.Xid
	req := opReq.Req.(*GetChildrenReq)
	if err := newErrorRepIfInvalidPath(xid, 0, req.Path); err != nil {
		return err
	}

	children, err := c.GetChildren(req.Path.Value)
	if (err != nil) {
		return newBackendErrorRep(xid, 0, err)
	}

	return newRep(
		xid, 0, errOk,
		&GetChildrenRep { Children: children },
	)
}

func processSyncReq(opReq OpReq) *OpRep {
	xid := opReq.Hdr.Xid
	req := opReq.Req.(*SyncReq)
	if err := newErrorRepIfInvalidPath(xid, 0, req.Path); err != nil {
		return err
	}

	return newRep(
		xid, 0, errOk,
		&SyncRep { Path: req.Path.Value },
	)
}

func processPingReq(OpReq) *OpRep {
	return newRep(
		-2, 0, errOk,
		&PingRep {},
	)
}

func processGetChildren2Req(opReq OpReq, c backends.Client) *OpRep {
	xid := opReq.Hdr.Xid
	req := opReq.Req.(*GetChildren2Req)
	if err := newErrorRepIfInvalidPath(xid, 0, req.Path); err != nil {
		return err
	}

	children, err := c.GetChildren(req.Path.Value)
	if (err != nil) {
		return newBackendErrorRep(xid, 0, err)
	}

	return newRep(
		xid, 0, errOk,
		&GetChildren2Rep {
			Children: children,
			Stat: newStat(0, 0, 0),
		},
	)
}

func processCheckVersionReq(OpReq) *OpRep {
	return nil
}

func processMultiReq(opReq OpReq) *OpRep {
	return newErrorRep(opReq.Hdr.Xid, 0, errUnimplemented)
}

func processCreate2Req(opReq OpReq, c backends.Client) *OpRep {
	xid := opReq.Hdr.Xid
	req := opReq.Req.(*Create2Req)
	if err := newErrorRepIfInvalidPath(xid, 0, req.Path); err != nil {
		return err
	}

	err := c.Create(req.Path.Value, string(req.Data))
	if (err != nil) {
		return newBackendErrorRep(xid, 0, err)
	}

	return newRep(
		xid, 0, errOk,
		&Create2Rep {
			Path: req.Path.Value,
			Stat: newStat(0, 0, 0),
		},
	)
}

func processCloseReq(opReq OpReq) *OpRep {
	xid := opReq.Hdr.Xid
	return newRep(
		xid, 0, errOk,
		&CloseRep {},
	)
}

func processSetAuthReq(opReq OpReq) *OpRep {
	return newErrorRep(opReq.Hdr.Xid, 0, errUnimplemented)
}

func processSetWatchesReq(opReq OpReq) *OpRep {
	return newErrorRep(opReq.Hdr.Xid, 0, errUnimplemented)
}
