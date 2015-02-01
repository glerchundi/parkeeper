package keeper

import (
	"fmt"

	kv "github.com/glerchundi/kvstores"
	"github.com/glerchundi/parkeeper/log"
)

var processorByOpCode = map[int32]func(OpReq, kv.Client)*OpRep {
	opCreate: func (opReq OpReq, client kv.Client) *OpRep {
		return processCreateReq(opReq, client)
	},
	opDelete: func (opReq OpReq, client kv.Client) *OpRep {
		return processDeleteReq(opReq, client)
	},
	opExists: func (opReq OpReq, client kv.Client) *OpRep {
		return processExistsReq(opReq, client)
	},
	opGetData: func (opReq OpReq, client kv.Client) *OpRep {
		return processGetDataReq(opReq, client)
	},
	opSetData: func (opReq OpReq, client kv.Client) *OpRep {
		return processSetDataReq(opReq, client)
	},
	opGetAcl: func (opReq OpReq, _ kv.Client) *OpRep {
		return processGetAclReq(opReq)
	},
	opSetAcl: func (opReq OpReq, _ kv.Client) *OpRep {
		return processSetAclReq(opReq)
	},
	opGetChildren: func (opReq OpReq, client kv.Client) *OpRep {
		return processGetChildrenReq(opReq, client)
	},
	opSync: func (opReq OpReq, _ kv.Client) *OpRep {
		return processSyncReq(opReq)
	},
	opPing: func (opReq OpReq, _ kv.Client) *OpRep {
		return processPingReq(opReq)
	},
	opGetChildren2: func (opReq OpReq, client kv.Client) *OpRep {
		return processGetChildren2Req(opReq, client)
	},
	opCheck: func (opReq OpReq, client kv.Client) *OpRep {
		return processCheckVersionReq(opReq, client)
	},
	opMulti: func (opReq OpReq, _ kv.Client) *OpRep {
		return processMultiReq(opReq)
	},
	opCreate2: func (opReq OpReq, client kv.Client) *OpRep {
		return processCreate2Req(opReq, client)
	},
	opClose: func (opReq OpReq, _ kv.Client) *OpRep {
		return processCloseReq(opReq)
	},
	opSetAuth: func (opReq OpReq, _ kv.Client) *OpRep {
		return processSetAuthReq(opReq)
	},
	opSetWatches: func (opReq OpReq, _ kv.Client) *OpRep {
		return processSetWatchesReq(opReq)
	},
}

var keeperErrFromBackendErr = map[int]int32 {
	kv.Unknown:            errSystemError,
	kv.Unimplemented:      errUnimplemented,
	kv.BackendUnreachable: errSystemError,
	kv.KeyNotFound:        errNoNode,
	kv.KeyExists:          errNodeExists,
	kv.BadVersion:         errBadVersion,
}

func mapBackendError(err *kv.Error) int32 {
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

func newBackendErrorRep(xid int32, zxid int64, err *kv.Error) *OpRep {
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

func processOpReq(opReq OpReq, storeClient kv.Client, sendChan chan Rep) {
	// find processor
	processor, found := processorByOpCode[opReq.Hdr.OpCode]
	if !found {
		log.Error(fmt.Sprintf("cannot process opcode: %d", opReq.Hdr.OpCode))
		return
	}

	// process request & write rep (if needed)
	rep := processor(opReq, storeClient)
	if (rep != nil) {
		sendChan <- rep
	}
}

func processCreateReq(opReq OpReq, client kv.Client) *OpRep {
	xid := opReq.Hdr.Xid
	req := opReq.Req.(*CreateReq)
	if err := newErrorRepIfInvalidPath(xid, 0, req.Path); err != nil {
		return err
	}

	err := client.Create(req.Path.Value, string(req.Data))
	if (err != nil) {
		return newBackendErrorRep(xid, 0, err)
	}

	return newRep(
		xid, 0, errOk,
		&CreateRep { Path: req.Path.Value },
	)
}

func processDeleteReq(opReq OpReq, client kv.Client) *OpRep {
	xid := opReq.Hdr.Xid
	req := opReq.Req.(*DeleteReq)
	if err := newErrorRepIfInvalidPath(xid, 0, req.Path); err != nil {
		return err
	}

	err := client.Delete(req.Path.Value, req.Version)
	if (err != nil) {
		return newBackendErrorRep(xid, 0, err)
	}

	return newRep(
		xid, 0, errOk,
		&DeleteRep {},
	)
}

func processExistsReq(opReq OpReq, client kv.Client) *OpRep {
	xid := opReq.Hdr.Xid
	req := opReq.Req.(*ExistsReq)
	if err := newErrorRepIfInvalidPath(xid, 0, req.Path); err != nil {
		return err
	}

	err := client.Exists(req.Path.Value)
	if (err != nil) {
		return newBackendErrorRep(xid, 0, err)
	}

	return newRep(
		xid, 0, errOk,
		&ExistsRep { Stat: newStat(0, 0, 0) },
	)
}

func processGetDataReq(opReq OpReq, client kv.Client) *OpRep {
	xid := opReq.Hdr.Xid
	req := opReq.Req.(*GetDataReq)
	if err := newErrorRepIfInvalidPath(xid, 0, req.Path); err != nil {
		return err
	}

	node, err := client.GetData(req.Path.Value)
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

func processSetDataReq(opReq OpReq, client kv.Client) *OpRep {
	xid := opReq.Hdr.Xid
	req := opReq.Req.(*SetDataReq)
	if err := newErrorRepIfInvalidPath(xid, 0, req.Path); err != nil {
		return err
	}

	err := client.SetData(req.Path.Value, string(req.Data), req.Version)
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

func processGetChildrenReq(opReq OpReq, client kv.Client) *OpRep {
	xid := opReq.Hdr.Xid
	req := opReq.Req.(*GetChildrenReq)
	if err := newErrorRepIfInvalidPath(xid, 0, req.Path); err != nil {
		return err
	}

	children, err := client.GetChildren(req.Path.Value)
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

func processGetChildren2Req(opReq OpReq, client kv.Client) *OpRep {
	xid := opReq.Hdr.Xid
	req := opReq.Req.(*GetChildren2Req)
	if err := newErrorRepIfInvalidPath(xid, 0, req.Path); err != nil {
		return err
	}

	children, err := client.GetChildren(req.Path.Value)
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

func processCheckVersionReq(opReq OpReq, client kv.Client) *OpRep {
	xid := opReq.Hdr.Xid
	req := opReq.Req.(*CheckVersionReq)
	if err := newErrorRepIfInvalidPath(xid, 0, req.Path); err != nil {
		return err
	}

	node, err := client.GetData(req.Path.Value)
	if (err != nil) {
		return newBackendErrorRep(xid, 0, err)
	}

	if (req.Version != int32(node.ModifiedIndex)) {
		return newErrorRep(xid, 0, errBadVersion)
	}

	return newRep(
		xid, 0, errOk,
		&PingRep {},
	)
}

func processMultiReq(opReq OpReq) *OpRep {
	return newErrorRep(opReq.Hdr.Xid, 0, errUnimplemented)
}

func processCreate2Req(opReq OpReq, client kv.Client) *OpRep {
	xid := opReq.Hdr.Xid
	req := opReq.Req.(*Create2Req)
	if err := newErrorRepIfInvalidPath(xid, 0, req.Path); err != nil {
		return err
	}

	err := client.Create(req.Path.Value, string(req.Data))
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

