package keeper

import (
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"runtime"

	"github.com/glerchundi/parkeeper/log"
)

//
// Common structs
//

type Rep interface{}
type Req interface{}

type OpReq struct {
	Hdr *OpReqHeader
	Req interface{}
}

type OpRep struct {
	Hdr *OpRepHeader
	Rep interface{}
}

type OpReqHeader struct {
	Xid    int32
	OpCode int32
}

type OpRepHeader struct {
	Xid  int32
	Zxid int64
	Err  int32
}

type Id struct {
	Scheme string
	Id     string
}

type Acl struct {
	Perms int32
	Id    Id
}

type Stat struct {
	CreatedZxid      int64 // The zxid of the change that caused this znode to be created.
	ModifiedZxid     int64 // The zxid of the change that last modified this znode.
	CreatedTime      int64 // The time in milliseconds from epoch when this znode was created.
	ModifiedTime     int64 // The time in milliseconds from epoch when this znode was last modified.
	Version          int32 // The number of changes to the data of this znode.
	ChildrenVersion  int32 // The number of changes to the children of this znode.
	AclVersion       int32 // The number of changes to the ACL of this znode.
	EphemeralOwner   int64 // The session id of the owner of this znode if the znode is an ephemeral
	                       // node. If it is not an ephemeral node, it will be zero.
	DataLength       int32 // The length of the data field of this znode.
	NumChildren      int32 // The number of children of this znode.
	Pzxid            int64 // The zxid of the change that last modified children of this znode.
}

// Taken from: https://zookeeper.apache.org/doc/r3.1.2/zookeeperProgrammers.html
// The ZooKeeper Data Model
// ZooKeeper has a hierarchal name space, much like a distributed file system. The only difference is that each node in
// the namespace can have data associated with it as well as children. It is like having a file system that allows a
// file to also be a directory. Paths to nodes are always expressed as canonical, absolute, slash-separated paths;
// there are no relative reference. Any unicode character can be used in a path subject to the following constraints:

type Path struct {
	Value   string
	isValid bool
}

func (p *Path) Decode(buf []byte) (int, error) {
	return decodePacketValue(buf, reflect.ValueOf(&p.Value))
}

func (p *Path) Encode(buf []byte) (int, error) {
	return encodePacketValue(buf, reflect.ValueOf(&p.Value))
}

// Ported from: ZooKeeper's PathUtils.java
func (p *Path) Init() {
	p.isValid = false

	runeCount := 0
	runes := make([]rune, len(p.Value))
	for i, rune := range p.Value {
		runes[i] = rune
		runeCount = runeCount + 1
	}

	if (runeCount == 0) {
		log.Error("Path length must be > 0")
		return
	}

	if (runes[0] != '/') {
		log.Error("Path must start with / character")
		return
	}

	// done checking - it's the root
	if (runeCount == 1) {
		p.isValid = true
		return
	}

	if (runes[runeCount - 1] == '/') {
		log.Error("Path must not end with / character")
		return
	}

	lastRune := rune(0x0000)
	for i, rune := range runes {
		if (rune == 0x0000) {
			log.Error(fmt.Sprintf("null character not allowed @%d", i))
			return
		} else if (rune == '/' && lastRune == '/') {
			log.Error(fmt.Sprintf("empty node name specified @%d", i))
			return
		} else if (rune == '.' && lastRune == '.') {
			if (runes[i - 2] == '/' && ((i + 1 == runeCount) || runes[i + 1] == '/')) {
				log.Error(fmt.Sprintf("relative paths not allowed @%d", i))
				return
			}
		} else if (rune == '.') {
			if (runes[i - 1] == '/' && ((i + 1 == runeCount) || runes[i + 1] == '/')) {
				log.Error(fmt.Sprintf("relative paths not allowed @%d", i))
				return
			}
		} else if (rune  > 0x0000 && rune <= 0x001f || rune >= 0x007f && rune <= 0x009f ||
				   rune >= 0xd8f3 && rune <= 0xf8ff || rune >= 0xfff0 && rune <= 0xffff) {
			log.Error(fmt.Sprintf("invalid charater @%d", i))
			return
		}

		lastRune = rune
	}

	p.isValid = true
}

func (p *Path) IsValid() bool {
	return p.isValid
}

//
// Connect Req/Rep
//

type ConnectReq struct {
	ProtocolVersion int32
	LastZxidSeen    int64
	TimeOut         int32
	SessionId       int64
	Passwd          []byte
	ReadOnly        bool
}

type ConnectRep struct {
	ProtocolVersion int32
	TimeOut         int32
	SessionId       int64
	Passwd          []byte
}

//
// Notify Req/Rep
//

type NotifyReq struct {}
type NotifyRep struct {}

//
// Create Req/Rep
//

type CreateReq struct {
	Path  *Path
	Data  []byte
    Acls  []Acl
    Flags int32
}

type CreateRep struct {
	Path string
}

//
// Delete Req/Rep
//

type DeleteReq struct {
	Path    *Path
	Version int32
}

type DeleteRep struct {}

//
// Exists Req/Rep
//

type ExistsReq struct {
	Path  *Path
	Watch bool
}

type ExistsRep struct {
	Stat Stat
}

//
// GetData Req/Rep
//

type GetDataReq struct {
	Path  *Path
	Watch bool
}

type GetDataRep struct {
	Data []byte
	Stat Stat
}

//
// SetData Req/Rep
//

type SetDataReq struct {
	Path    *Path
	Data    []byte
	Version int32
}

type SetDataRep struct {
	Stat Stat
}

//
// GetAcl Req/Rep
//

type GetAclReq struct {
	Path *Path
}

type GetAclRep struct {
    Acls []Acl
	Stat Stat
}

//
// SetAcl Req/Rep
//

type SetAclReq struct {
	Path    *Path
	Acls    []Acl
    Version int32
}

type SetAclRep struct {
	Stat Stat
}

//
// GetChildren Req/Rep
//

type GetChildrenReq struct {
	Path  *Path
	Watch bool
}

type GetChildrenRep struct {
	Children []string
}

//
// Sync Req/Rep
//

type SyncReq struct {
	Path *Path
}

type SyncRep struct {
	Path string
}

//
// Ping Req/Rep
//

type PingReq struct {}
type PingRep struct {}

//
// GetChildren2 Req/Rep
//

type GetChildren2Req struct {
	Path  *Path
	Watch bool
}

type GetChildren2Rep struct {
	Children []string
	Stat     Stat
}

//
// CheckVersion Req/Rep
//

type CheckVersionReq struct {
	Path    *Path
	Version int32
}

type CheckVersionRep struct {}

//
// Multi Req/Rep
//

type MultiHeader struct {
	OpoCde int32
	Done   bool
	Err    int32
}

type MultiReqOp struct {
	Hdr MultiHeader
	Op  interface{}
}

type MultiRepOp struct {
	Hdr    MultiHeader
	String string
	Stat   Stat
}

type MultiReq struct {
	Ops        []MultiReqOp
	DoneHeader MultiHeader
}

type MultiRep struct {
	Ops        []MultiRepOp
	DoneHeader MultiHeader
}

//
// Create2 Req/Rep
//

type Create2Req struct {
	Path  *Path
	Data  []byte
	Acls  []Acl
	Flags int32
}

type Create2Rep struct {
	Path string
	Stat Stat
}

//
// Close Req/Rep
//

type CloseReq struct {}
type CloseRep struct {}

//
// SetAuth Req/Rep
//

type SetAuthReq struct {
    Type   int32
	Scheme string
	Auth   []byte
}

type SetAuthRep struct {}

//
// SetWatches Req/Rep
//

type SetWatchesReq struct {
	RelativeZxid int64
	DataWatches  []string
	ExistWatches []string
	ChildWatches []string
}

type SetWatchesRep struct {}

//
// Request creator map
//

var creatorByOpCode = map[int32]func()Req {
	opCreate:       func() Req { return &CreateReq{} },
	opDelete:       func() Req { return &DeleteReq{} },
	opExists:       func() Req { return &ExistsReq{} },
	opGetData:      func() Req { return &GetDataReq{} },
	opSetData:      func() Req { return &SetDataReq{} },
	opGetAcl:       func() Req { return &GetAclReq{} },
	opSetAcl:       func() Req { return &SetAclReq{} },
	opGetChildren:  func() Req { return &GetChildrenReq{} },
	opSync:         func() Req { return &SyncReq{} },
	opPing:         func() Req { return &PingReq{} },
	opGetChildren2: func() Req { return &GetChildren2Req{} },
	opCheck:        func() Req { return &CheckVersionReq{} },
	opMulti:        func() Req { return &MultiReq{} },
	opCreate2:      nil,
	opClose:        func() Req { return &CloseReq{} },
	opSetAuth:      func() Req { return &SetAuthReq{} },
	opSetWatches:   func() Req { return &SetWatchesReq{} },
}

//
// Generic Encoding/Decoding methods thanks to samuel@descolada.com (http://samuelks.com). Used
// in go-zookeeper project (https://github.com/samuel/go-zookeeper)
// Such a good piece of code!!
//

var (
	ErrUnhandledFieldType = errors.New("unhandled field type")
	ErrPtrExpected        = errors.New("encode/decode expect a non-nil pointer to struct")
	ErrShortBuffer        = errors.New("buffer too small")
)

type Decoder interface {
	Decode(buf []byte) (int, error)
}

type Encoder interface {
	Encode(buf []byte) (int, error)
}

type Initializer interface {
	Init()
}

func DecodePacket(buf []byte, st interface{}) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(runtime.Error); ok && e.Error() == "runtime error: slice bounds out of range" {
				err = ErrShortBuffer
			} else {
				panic(r)
			}
		}
	}()

	v := reflect.ValueOf(st)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return 0, ErrPtrExpected
	}
	return decodePacketValue(buf, v)
}

func decodePacketValue(buf []byte, v reflect.Value) (int, error) {
	rv := v
	kind := v.Kind()
	if kind == reflect.Ptr {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
		kind = v.Kind()
	}

	var n int = 0
	var err error = nil
	switch kind {
	default:
		return n, ErrUnhandledFieldType
	case reflect.Struct:
		if de, ok := rv.Interface().(Decoder); ok {
			n, err = de.Decode(buf)
		} else if de, ok := v.Interface().(Decoder); ok {
			n, err = de.Decode(buf)
		} else {
			for i := 0; i < v.NumField(); i++ {
				field := v.Field(i)
				n2, err := decodePacketValue(buf[n:], field)
				n += n2
				if err != nil {
					break
				}
			}
		}

		if (err != nil) {
			return n, err
		}

		if i, ok := rv.Interface().(Initializer); ok {
			i.Init()
		} else if i, ok := v.Interface().(Initializer); ok {
			i.Init()
		}
	case reflect.Bool:
		v.SetBool(buf[n] != 0)
		n++
	case reflect.Int32:
		v.SetInt(int64(binary.BigEndian.Uint32(buf[n : n+4])))
		n += 4
	case reflect.Int64:
		v.SetInt(int64(binary.BigEndian.Uint64(buf[n : n+8])))
		n += 8
	case reflect.String:
		ln := int(binary.BigEndian.Uint32(buf[n : n+4]))
		v.SetString(string(buf[n+4 : n+4+ln]))
		n += 4 + ln
	case reflect.Slice:
		switch v.Type().Elem().Kind() {
		default:
			count := int(binary.BigEndian.Uint32(buf[n : n+4]))
			n += 4
			values := reflect.MakeSlice(v.Type(), count, count)
			v.Set(values)
			for i := 0; i < count; i++ {
				n2, err := decodePacketValue(buf[n:], values.Index(i))
				n += n2
				if err != nil {
					return n, err
				}
			}
		case reflect.Uint8:
			ln := int(int32(binary.BigEndian.Uint32(buf[n : n+4])))
			if ln < 0 {
				n += 4
				v.SetBytes(nil)
			} else {
				bytes := make([]byte, ln)
				copy(bytes, buf[n+4:n+4+ln])
				v.SetBytes(bytes)
				n += 4 + ln
			}
		}
	}
	return n, nil
}

func EncodePacket(buf []byte, st interface{}) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(runtime.Error); ok && e.Error() == "runtime error: slice bounds out of range" {
				err = ErrShortBuffer
			} else {
				panic(r)
			}
		}
	}()

	v := reflect.ValueOf(st)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return 0, ErrPtrExpected
	}
	return encodePacketValue(buf, v)
}

func encodePacketValue(buf []byte, v reflect.Value) (int, error) {
	rv := v
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		v = v.Elem()
	}

	n := 0
	switch v.Kind() {
	default:
		return n, ErrUnhandledFieldType
	case reflect.Struct:
		if en, ok := rv.Interface().(Encoder); ok {
			return en.Encode(buf)
		} else if en, ok := v.Interface().(Encoder); ok {
			return en.Encode(buf)
		} else {
			for i := 0; i < v.NumField(); i++ {
				field := v.Field(i)
				n2, err := encodePacketValue(buf[n:], field)
				n += n2
				if err != nil {
					return n, err
				}
			}
		}
	case reflect.Bool:
		if v.Bool() {
			buf[n] = 1
		} else {
			buf[n] = 0
		}
		n++
	case reflect.Int32:
		binary.BigEndian.PutUint32(buf[n:n+4], uint32(v.Int()))
		n += 4
	case reflect.Int64:
		binary.BigEndian.PutUint64(buf[n:n+8], uint64(v.Int()))
		n += 8
	case reflect.String:
		str := v.String()
		binary.BigEndian.PutUint32(buf[n:n+4], uint32(len(str)))
		copy(buf[n+4:n+4+len(str)], []byte(str))
		n += 4 + len(str)
	case reflect.Slice:
		switch v.Type().Elem().Kind() {
		default:
			count := v.Len()
			startN := n
			n += 4
			for i := 0; i < count; i++ {
				n2, err := encodePacketValue(buf[n:], v.Index(i))
				n += n2
				if err != nil {
					return n, err
				}
			}
			binary.BigEndian.PutUint32(buf[startN:startN+4], uint32(count))
		case reflect.Uint8:
			if v.IsNil() {
				binary.BigEndian.PutUint32(buf[n:n+4], uint32(0xffffffff))
				n += 4
			} else {
				bytes := v.Bytes()
				binary.BigEndian.PutUint32(buf[n:n+4], uint32(len(bytes)))
				copy(buf[n+4:n+4+len(bytes)], bytes)
				n += 4 + len(bytes)
			}
		}
	}
	return n, nil
}
