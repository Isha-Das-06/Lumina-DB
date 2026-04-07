package transport

import (
	"context"
	"fmt"
	"net"

	"github.com/user/luminadb/internal/raft"
	"github.com/user/luminadb/internal/storage"
	"github.com/user/luminadb/proto"
	"google.golang.org/grpc"
)

type Server struct {
	proto.UnimplementedLuminaServer
	proto.UnimplementedConsensusServer
	node   *raft.Node
	engine *storage.Engine
}

func NewServer(node *raft.Node, engine *storage.Engine) *Server {
	return &Server{
		node:   node,
		engine: engine,
	}
}

func (s *Server) Start(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterLuminaServer(grpcServer, s)
	proto.RegisterConsensusServer(grpcServer, s)

	fmt.Printf("LuminaDB server listening on %s\n", addr)
	return grpcServer.Serve(lis)
}

// --- Consensus Service ---

func (s *Server) RequestVote(ctx context.Context, req *proto.VoteRequest) (*proto.VoteResponse, error) {
	return s.node.HandleRequestVote(req), nil
}

func (s *Server) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	return s.node.HandleAppendEntries(req), nil
}

// --- Lumina (Client) Service ---

func (s *Server) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	fmt.Printf("Put request for key: %s\n", req.Key)
	success := s.node.Propose(req.Key, req.Value, proto.LogEntry_PUT)
	if !success {
		return &proto.PutResponse{
			Success:  false,
			LeaderId: s.node.GetLeader(),
		}, nil
	}
	return &proto.PutResponse{Success: true}, nil
}

func (s *Server) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	val, ok := s.engine.Get(req.Key)
	return &proto.GetResponse{Value: val, Found: ok}, nil
}

func (s *Server) Delete(ctx context.Context, req *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	fmt.Printf("Delete request for key: %s\n", req.Key)
	success := s.node.Propose(req.Key, nil, proto.LogEntry_DELETE)
	if !success {
		return &proto.DeleteResponse{Success: false}, nil
	}
	return &proto.DeleteResponse{Success: true}, nil
}
 Lands
