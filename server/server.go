package server

import (
	"context"

	api "github.com/CarlosLecval/log_server/api/v1"
	"github.com/CarlosLecval/log_server/log"
	"google.golang.org/grpc"
)

type Config struct {
	CommitLog *log.Log
}

type LogServiceServer struct {
	api.UnimplementedLogServiceServer
	CommitLog *log.Log
}

func NewGRPCServer(config *Config) (srv *grpc.Server, err error) {
	logService := &LogServiceServer{
		CommitLog: config.CommitLog,
	}
	var opts []grpc.ServerOption
	srv = grpc.NewServer(opts...)
	api.RegisterLogServiceServer(srv, logService)
	return srv, nil
}

func (s *LogServiceServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *LogServiceServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		re, ok := err.(*api.ErrOffsetOutOfRange)
		if ok {
			return nil, re.GRPCStatus().Err()
		}
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

func (s *LogServiceServer) ConsumeStream(req *api.ConsumeRequest, stream api.LogService_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}

func (s *LogServiceServer) ProduceStream(stream api.LogService_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}
