package main

import (
	"os"

	kv "github.com/glerchundi/kvstores"
	"github.com/glerchundi/parkeeper/keeper"
	"github.com/glerchundi/parkeeper/log"

	"github.com/codegangsta/cli"
)

func appMain(c *cli.Context) {
	// flags
	bindAddr := c.String("bind-addr")
	backendUrl := c.String("backend-url")

	// configure main logger
	log.SetLogger(log.NewLogger(false, true, true))

	// backend client
	storeClient, err := kv.NewClient(backendUrl)
	if err != nil {
		panic(err)
	}

	// start listening
	server := keeper.NewServer(bindAddr, storeClient)
	server.Start()
}

func main() {
	app := cli.NewApp()
	app.Name = "parkeeper"
	app.Version = releaseVersion
	app.Usage = "acts as a zookeeper service bridging all requests to etcd/consul"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "bind-addr",
			Value: "0.0.0.0:2181",
			Usage: "bind this server to a custom internal address",
		},
		cli.StringFlag{
			Name:  "backend-url",
			Value: "etcd://127.0.0.1:4001",
			Usage: "backend to use (etcd://127.0.0.1:4001, consul://127.0.0.1:8500)",
		},
	}
	app.Action = appMain
	app.Run(os.Args)
}
