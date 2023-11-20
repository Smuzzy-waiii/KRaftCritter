package main

import (
	"YAKT/FSM"
	"YAKT/helpers"
	rpc2 "YAKT/rpc"
	"context"
	"flag"
	"fmt"
	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	transport "github.com/Jille/raft-grpc-transport"
	"github.com/Jille/raftadmin"
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
	"os"
	"path/filepath"
)

var (
	grpcAddr = flag.String("address", "localhost:50051", "TCP host+port for this node")
	raftId   = flag.String("raft_id", "", "Node id used by Raft")

	raftDir       = flag.String("raft_data_dir", "data/", "Raft data dir")
	raftBootstrap = flag.Bool("raft_bootstrap", false, "Whether to bootstrap the Raft cluster")
)

func main() {
	flag.Parse()

	if *raftId == "" {
		log.Fatalf("flag --raft_id is required")
	}

	ctx := context.Background()
	_, port, err := net.SplitHostPort(*grpcAddr)
	if err != nil {
		log.Fatalf("failed to parse local address (%q): %v", *grpcAddr, err)
	}
	sock, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	fsm := &FSM.DistMap{}

	r, tm, err := NewRaft(ctx, *raftId, *grpcAddr, fsm)
	if err != nil {
		log.Fatalf("failed to start raft: %v", err)
	}

	//Running grpc server
	go func() {
		s := grpc.NewServer()
		tm.Register(s)
		leaderhealth.Setup(r, s, []string{"Example"})
		raftadmin.Register(s, r)
		reflection.Register(s)
		fmt.Println("Starting grpc Server on port", port)
		if err := s.Serve(sock); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	//Running HTTP Server
	go func() {
		rpc := rpc2.RpcInterface{Raft: r, Fsm: fsm}

		router := gin.Default()
		router.Use(rpc.RedirectLeader())

		router.POST("/brokers", rpc.RegisterBroker)
		router.GET("/activeBrokers", rpc.GetAllActiveBrokers)
		router.GET("/brokers", rpc.GetBrokers)
		router.DELETE("/brokers", rpc.DeleteBroker)
		router.PUT("/brokers", rpc.ReplaceBroker)
		router.POST("/topics", rpc.CreateTopic)
		router.GET("/topics", rpc.GetTopics)
		router.POST("/producers", rpc.RegisterProducer)
		router.POST("/partitions", rpc.CreatePartition)
		router.GET("/clientMetadata", rpc.GetClientMetadata)
		router.GET("/brokerMetadata", rpc.GetBrokerMetadata)

		host, httpPort, err := helpers.GetHttpAddrFromGrpcAddr(*grpcAddr)
		if err != nil {
			log.Fatalf("failed to parse local address (%q): %v", *grpcAddr, err)
		}

		fmt.Printf("Started HTTP Sever on %s:%s\n", host, httpPort)
		router.Run(fmt.Sprintf("%s:%s", host, httpPort))
	}()

	select {}
}

func NewRaft(ctx context.Context, myID, myAddress string, fsm raft.FSM) (*raft.Raft, *transport.Manager, error) {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(myID)

	baseDir := filepath.Join(*raftDir, myID)
	err := os.MkdirAll(baseDir, os.ModePerm)
	if err != nil {
		return nil, nil, fmt.Errorf("Cannot create path %s: %v", baseDir, err)
	}

	ldb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "logs.dat"))
	if err != nil {
		return nil, nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "logs.dat"), err)
	}

	sdb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "stable.dat"))
	if err != nil {
		return nil, nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "stable.dat"), err)
	}

	fss, err := raft.NewFileSnapshotStore(baseDir, 3, os.Stderr)
	if err != nil {
		return nil, nil, fmt.Errorf(`raft.NewFileSnapshotStore(%q, ...): %v`, baseDir, err)
	}

	tm := transport.New(raft.ServerAddress(myAddress), []grpc.DialOption{grpc.WithInsecure()})

	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, tm.Transport())
	if err != nil {
		return nil, nil, fmt.Errorf("raft.NewRaft: %v", err)
	}

	if *raftBootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(myID),
					Address:  raft.ServerAddress(myAddress),
				},
			},
		}
		f := r.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			return nil, nil, fmt.Errorf("raft.Raft.BootstrapCluster: %v", err)
		}
	}

	return r, tm, nil
}
