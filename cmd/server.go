package main

import (
	"fmt"
	"github.com/CarlosLecval/log_server/internal/auth"
	tlsconfig "github.com/CarlosLecval/log_server/internal/config"
	"github.com/CarlosLecval/log_server/internal/log"
	"github.com/CarlosLecval/log_server/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"net"
	"os"
)

func main() {
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Printf("failed to listen: %v", err)
		return
	}

	severTLSConfig, err := tlsconfig.SetupTLSConfig(tlsconfig.TLSConfig{
		CertFile: tlsconfig.ServerCertFile,
		KeyFile:  tlsconfig.ServerKeyFile,
		CAFile:   tlsconfig.CAFile,
		Server:   true,
	})
	if err != nil {
		fmt.Printf("failed to setup tls: %v", err)
		return
	}

	serverCreds := credentials.NewTLS(severTLSConfig)

	dir, err := os.MkdirTemp("", "server-test")
	if err != nil {
		fmt.Printf("failed to create temp dir: %v", err)
		return
	}

	clog, err := log.NewLog(dir, log.Config{})
	if err != nil {
		fmt.Printf("failed to create commit log: %v", err)
		return
	}

	authorizer := auth.New(tlsconfig.ACLModelFile, tlsconfig.ACLPolicyFile)

	config := &server.Config{
		CommitLog:  clog,
		Authorizer: authorizer,
	}
	server, err := server.NewGRPCServer(config, grpc.Creds(serverCreds))
	if err != nil {
		fmt.Printf("failed to create server: %v", err)
		return
	}
	fmt.Printf("server listening at %v", lis.Addr())
	if err := server.Serve(lis); err != nil {
		fmt.Printf("failed to serve: %v", err)
		return
	}
}
