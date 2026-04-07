package transport

import (
	"context"
	"time"

	"github.com/user/luminadb/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RaftClient handles outgoing RPCs to other Raft nodes
type RaftClient struct {
	conns map[string]*grpc.ClientConn
}

func NewRaftClient() *RaftClient {
	return &RaftClient{
		conns: make(map[string]*grpc.ClientConn),
	}
}

func (c *RaftClient) getConn(peer string) (proto.ConsensusClient, error) {
	if conn, ok := c.conns[peer]; ok {
		return proto.NewConsensusClient(conn), nil
	}

	conn, err := grpc.Dial(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.conns[peer] = conn
	return proto.NewConsensusClient(conn), nil
}

func (c *RaftClient) RequestVote(peer string, req *proto.VoteRequest) (*proto.VoteResponse, error) {
	client, err := c.getConn(peer)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	return client.RequestVote(ctx, req)
}

func (c *RaftClient) AppendEntries(peer string, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	client, err := c.getConn(peer)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	return client.AppendEntries(ctx, req)
}
 Lands
