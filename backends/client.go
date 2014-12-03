package backends

import (
	"errors"
	"net"
	"net/url"
	"strings"
	"strconv"
)

const (
	Unknown            = 1
	Unimplemented      = 2
	BackendUnreachable = 3
	KeyNotFound        = 4
	KeyExists          = 5
	BadVersion         = 6
)

var errCodeToErrMsg = map[int]string {
	Unknown:            "unknown",
	Unimplemented:      "unimplemented",
	BackendUnreachable: "backend unreachable",
	KeyNotFound:        "key not found",
	KeyExists:          "key exists",
	BadVersion:         "bad version",
}

type Error struct {
	errCode int
	errMsg  string
}

func (e *Error) Code() int {
	return e.errCode
}

func (e *Error) Error() string {
	if (e.errMsg != "") {
		return e.errMsg
	}

	msg, found := errCodeToErrMsg[e.errCode]
	if (!found) {
		return "unable to identify error"
	}

	return msg
}

type Node struct {
	Path          string
	Value         string
	CreatedIndex  uint64
	ModifiedIndex uint64
	Nodes         Nodes
}

type Nodes []*Node

// normalizeAddress returns addr with the passed default port appended if
// there is not already a port specified.
func NormalizeAddress(addr string, defaultPort uint16) string {
	_, _, err := net.SplitHostPort(addr)
	if err != nil {
		return net.JoinHostPort(addr, strconv.Itoa(int(defaultPort)))
	}
	return addr
}

type Client interface {
	Create(path string, data string) *Error
	Delete(path string, version int32) *Error
	Exists(path string) *Error
	GetData(path string) (*Node, *Error)
	SetData(path string, data string, version int32) *Error
	GetChildren(path string) ([]string, *Error)
}

func NewClient(backendUrl string) (Client, error) {
	// parse url
	u, err := url.Parse(backendUrl)
	if err != nil {
		return nil, err
	}

	// create and return backend client
	var addr string = u.Host
	switch strings.ToLower(u.Scheme) {
	case "etcd":
		addr = NormalizeAddress(addr, uint16(4001))
		return NewEtcdClient(addr)
	case "consul":
		addr = NormalizeAddress(addr, uint16(8500))
		return NewConsulClient(addr)
	}

	// return constructed client
	return nil, errors.New("Invalid backend")
}
