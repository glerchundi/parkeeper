# parkeeper [![Build Status](https://travis-ci.org/glerchundi/parkeeper.svg?branch=master)](https://travis-ci.org/glerchundi/parkeeper) [![Docker Repository on Quay.io](https://quay.io/repository/glerchundi/parkeeper/status "Docker Repository on Quay.io")](https://quay.io/repository/glerchundi/parkeeper)

One service discovery backend to rule them all. The idea behind this was to seamlessly use tools and frameworks that heavily rely on zookeeper, for example, [finagle](https://twitter.github.io/finagle/), [kafka](http://kafka.apache.org/), and keep/support/maintain just one key-value store. This could facilitate the migration to the nextgen service discovery/distributed configuration services like [etcd](https://github.com/coreos/etcd) or [consul](http://consul.io).

One of these two backends are available:
* etcd
* consul

Unsupported ZooKeeper features (ordered by priority):
- [ ] Reliable zxid (X-Consul-Index & X-Etcd-Index)
- [ ] Watches
- [ ] Ephemeral Nodes
- [ ] Sequence Nodes
- [ ] ACLs
- [ ] Auth
- [ ] Process requests in batch (Multi)
- [ ] Reliable Stats (?)

Listing of supported requests with some notes:

|              | etcd               | consul             |
| ------------ |:------------------:|:------------------:|
| CREATE       | :white_check_mark: <sup>1</sup> | :white_check_mark: |
| DELETE       | :white_check_mark: | :white_check_mark: <sup>2</sup>  |
| EXISTS       | :white_check_mark: | :white_check_mark: |
| GETDATA      | :white_check_mark: | :white_check_mark: |
| SETDATA      | :white_check_mark: | :white_check_mark: |
| GETACL       | :construction: | :construction: |
| SETACL       | :construction: | :construction: |
| GETCHILDREN  | :white_check_mark: | :white_check_mark: |
| SYNC         | :white_check_mark: <sup>3</sup> | :white_check_mark: <sup>3</sup> |
| PING         | :white_check_mark: | :white_check_mark: |
| GETCHILDREN2 | :white_check_mark: | :white_check_mark: |
| CHECK        | :white_check_mark: | :white_check_mark: |
| MULTI        | :construction: | :construction: |
| CREATE2      | :white_check_mark:<sup>1</sup> | :white_check_mark: |
| CLOSE        | :white_check_mark: | :white_check_mark: |
| SETAUTH      | :construction: | :construction: |
| SETWATCHES   | :construction: | :construction: |

<sup>1</sup> Unable to create a node with a key/path equal to an existing directory. (etcd will support this in v3 api: [#1855](https://github.com/coreos/etcd/issues/1855))

<sup>2</sup> Cannot delete a node with a specific version: https://github.com/hashicorp/consul/issues/348.

<sup>3</sup> There is no similar etcd/consul request. For now, it does not proceed.

Using parkeeper is as easy as this:

```bash
docker run -p 2181:2181 quay.io/glerchundi/parkeeper -backend-url etcd://127.0.0.1:4001
docker run -p 2181:2181 quay.io/glerchundi/parkeeper -backend-url consul://127.0.0.1:8500
```

This project is in its early stages, use at your own risk. And of course, any feedback is appreciated as well as issues!
