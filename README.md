# parkeeper [![Build Status](https://travis-ci.org/glerchundi/parkeeper.svg?branch=master)](https://travis-ci.org/glerchundi/parkeeper) [![Docker Repository on Quay.io](https://quay.io/repository/glerchundi/parkeeper/status "Docker Repository on Quay.io")](https://quay.io/repository/glerchundi/parkeeper)

One service discovery backend to rule them all. The idea behind this was to seamlessly use tools and frameworks that heavily rely on zookeeper, for example, [finagle](https://twitter.github.io/finagle/), [kafka](http://kafka.apache.org/), and keep/support/maintain just one key-value store. This could facilitate the migration to the nextgen service discovery/distributed configuration services like [etcd](https://github.com/coreos/etcd) or [consul](http://consul.io).

One of these two backends are available:
* etcd
* consul

Unsupported ZooKeeper features:
- [ ] Ephemeral Nodes
- [ ] Sequence Nodes
- [ ] Reliable zxid (?)
- [ ] Reliable Stats (?)
- [ ] Process requests in batch (Multi)
- [ ] Watches
- [ ] ACLs
- [ ] Auth

So ... is there something supported?

Here is a list of supported request with some notes:

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
| SYNC         | :question: | :question: |
| PING         | :white_check_mark: | :white_check_mark: |
| GETCHILDREN2 | :white_check_mark: | :white_check_mark: |
| CHECK        | :question: | :question: |
| MULTI        | :construction: | :construction: |
| CREATE2      | :white_check_mark:<sup>1</sup> | :white_check_mark: |
| CLOSE        | :white_check_mark: | :white_check_mark: |
| SETAUTH      | :construction: | :construction: |
| SETWATCHES   | :construction: | :construction: |

<sup>1</sup> Unable to create a node with a key/path equal to an existing directory.

<sup>2</sup> Cannot delete a node with a specific version: https://github.com/hashicorp/consul/issues/348.

Using parkeeper is as easy as this:

```bash
docker run -p 2181:2181 glerchundi/parkeeper -backend-url etcd://127.0.0.1:4001
docker run -p 2181:2181 glerchundi/parkeeper -backend-url consul://127.0.0.1:8500
```

This project is in its early stages, use at your own risk. And of course, any feedback is appreciated as well as issues!
