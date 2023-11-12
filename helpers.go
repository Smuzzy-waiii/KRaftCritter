package main

import (
	"fmt"
	"net"
	"strconv"
)

func getHttpAddrFromGrpcAddr(grpcAddr string) (host string, httpPort string, err error) {
	host, grpcPort, err := net.SplitHostPort(grpcAddr)
	if err != nil {
		return "", "", err
	}
	int_grpc_port, err := strconv.Atoi(grpcPort)
	if err != nil {
		return "", "", err
	}
	httpPort = fmt.Sprintf("%d", (int_grpc_port - 10000))
	return host, httpPort, nil
}
