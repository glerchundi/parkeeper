package kvstores

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	api "github.com/coreos/go-etcd/etcd"
)

type EtcdClient struct {
	addr   string
	client *api.Client
}

func NewEtcdClient(addr string, dialTimeout time.Duration) (*EtcdClient, error) {
	var c *api.Client
	/*
		var err error
		if cert != "" && key != "" {
			c, err = etcd.NewTLSClient(machines, cert, key, caCert)
			if err != nil {
				return &Client{c}, err
			}
		} else {
			c = etcd.NewClient(machines)
		}
	*/

	// machine addresses
	machines := []string{addr}

	// create custom client
	c = api.NewClient(machines)
	if !c.SetCluster(machines) {
		return nil, errors.New("cannot connect to etcd cluster: " + addr)
	}

	// configure dial timeout
	c.SetDialTimeout(dialTimeout)

	return &EtcdClient { addr: addr, client: c }, nil
}

func (c *EtcdClient) Create(path string, data string) (err *Error) {
	_, err = rawCall(func() (*api.RawResponse, error) {
		return c.client.RawCreate(path, data, 0)
	}, nil)
	return
}

func (c *EtcdClient) Delete(path string, version int32) (err *Error) {
	_, err = rawCall(func() (*api.RawResponse, error) {
		if (version == -1) {
			return c.client.RawDelete(path, false, false)
		} else {
			return c.client.RawCompareAndDelete(path, "", uint64(version))
		}
	}, nil)
	return
}

func (c *EtcdClient) Exists(path string) (err *Error) {
	_, err = rawCall(func() (*api.RawResponse, error) {
		return c.client.RawGet(path, true, false)
	}, nil)
	return
}

func (c *EtcdClient) GetData(path string) (*Node, *Error) {
	return rawCall(
		func() (*api.RawResponse, error) {
			return c.client.RawGet(path, true, false)
		},
		func(resp *api.Response) *Error {
			if (resp.Node.Dir) {
				return &Error { code: KeyNotFound }
			}
			return nil
		},
	)
}

func (c *EtcdClient) SetData(path string, data string, version int32) (err *Error) {
	_, err = rawCall(func() (*api.RawResponse, error) {
		if (version == -1) {
			return c.client.RawUpdate(path, data, 0)
		} else {
			return c.client.RawCompareAndSwap(path, data, 0, "", uint64(version))
		}
	}, nil)
	return
}

func (c *EtcdClient) GetChildren(path string) ([]string, *Error) {
	node, err := rawCall(func() (*api.RawResponse, error) {
		return c.client.RawGet(path, true, false)
	}, nil)
	if (err != nil) {
		return nil, err
	}

	numChildren := len(node.Nodes)
	children := make([]string, len(node.Nodes))
	for i := 0; i < numChildren; i++ {
		children[i] = node.Nodes[i].Path
	}

	return children, nil
}

func mapNode(etcdNode *api.Node) *Node {
	node := &Node{
		Path:          etcdNode.Key,
		Value:         etcdNode.Value,
		CreatedIndex:  etcdNode.CreatedIndex,
		ModifiedIndex: etcdNode.ModifiedIndex,
	}
	numChildren := len(etcdNode.Nodes)
	node.Nodes = make(Nodes, numChildren)
	for i := 0; i < numChildren; i++ {
		node.Nodes[i] = mapNode(etcdNode.Nodes[i])
	}

	return node
}

func rawCall(f func() (*api.RawResponse, error), v func(*api.Response) *Error) (*Node, *Error) {
	rawResp, cerr := f()
	if cerr != nil {
		return nil, &Error { code: BackendUnreachable, msg: cerr.Error() }
	}

	if rawResp.StatusCode != http.StatusOK && rawResp.StatusCode != http.StatusCreated {
		etcdError := new(api.EtcdError)
		json.Unmarshal(rawResp.Body, etcdError)

		var code int = Unknown
		switch etcdError.ErrorCode {
		case 100: // EcodeKeyNotFound
			code = KeyNotFound
		case 101: // EcodeTestFailed
			code = BadVersion
		case 102: // EcodeNotFile
			code = KeyNotFound
		case 105: // EcodeNodeExist
			code = KeyExists
		case 107: // EcodeRootROnly
			// TODO: Decide which error should be triggered
			code = KeyNotFound
		default:
			log.Printf("unhandled error: http: %d, etcd: %d", rawResp.StatusCode, etcdError.ErrorCode)
		}

		return nil, &Error { code: code }
	}

	resp, cerr := rawResp.Unmarshal()
	if cerr != nil {
		return nil, &Error { code: Unknown }
	}

	if (v != nil) {
		err := v(resp)
		if err != nil {
			return nil, err
		}
	}

	return mapNode(resp.Node), nil
}
