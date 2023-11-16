package main

import (
	"bytes"
	"encoding/gob"
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

func gobEncode(obj any) ([]byte, error) {
	b := new(bytes.Buffer)
	e := gob.NewEncoder(b)
	err := e.Encode(obj)
	if err != nil {
		return nil, nil
	}
	return b.Bytes(), nil
}

func gobDecode[T interface{}](data []byte, dict *T) error {
	b := bytes.NewBuffer(data)
	d := gob.NewDecoder(b)
	err := d.Decode(dict)
	return err
}
