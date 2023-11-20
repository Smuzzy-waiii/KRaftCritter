package helpers

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
	"strconv"
)

func GetHttpAddrFromGrpcAddr(grpcAddr string) (host string, httpPort string, err error) {
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

func GobEncode(obj any) ([]byte, error) {
	b := new(bytes.Buffer)
	e := gob.NewEncoder(b)
	err := e.Encode(obj)
	if err != nil {
		return nil, nil
	}
	return b.Bytes(), nil
}

func GobDecode[T interface{}](data []byte, dict *T) error {
	b := bytes.NewBuffer(data)
	d := gob.NewDecoder(b)
	err := d.Decode(dict)
	return err
}

func Values[M ~map[K]V, K comparable, V any](m M) []V {
	r := make([]V, 0, len(m))
	for _, v := range m {
		r = append(r, v)
	}
	return r
}

func DeepCopyMap[M ~map[K]V, K comparable, V any](dest *M, src M) {
	for k, v := range src {
		(*dest)[k] = v
	}
}
