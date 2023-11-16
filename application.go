package main

import (
	"encoding/gob"
	"fmt"
	"github.com/hashicorp/raft"
	"io"
	"log"
)

// DistMap impl.  raft.FSM
type DistMap struct {
	distMap map[string]any
	brokers map[int]Broker
}

func (fsm *DistMap) InitIfNotInit() {
	if (*fsm).distMap == nil {
		(*fsm).distMap = make(map[string]any)
	}

	if (*fsm).brokers == nil {
		(*fsm).brokers = make(map[int]Broker)
	}
}

type ApplyRv struct {
	MetaData map[string]string
	Error    error
}

func (fsm *DistMap) Apply(l *raft.Log) interface{} {
	fsm.InitIfNotInit()

	logType := string(l.Extensions)
	switch logType {
	case "KeyValue":
		return fsm.ApplyKVStore(l)

	case "Broker":
		return fsm.ApplyBroker(l)
	}
	log.Fatalln("Log type not recognised")
	return ApplyRv{}
}

func (fsm *DistMap) Snapshot() (raft.FSMSnapshot, error) {
	distMapCopy := make(map[string]any)
	for k := range fsm.distMap {
		distMapCopy[k] = fsm.distMap[k]
	}
	brokerCopy := make(map[int]Broker)
	for k := range fsm.brokers {
		brokerCopy[k] = fsm.brokers[k]
	}
	return &snapshot{
		distMapCopy,
		brokerCopy,
	}, nil
}

func (fsm *DistMap) Restore(r io.ReadCloser) error {
	d := gob.NewDecoder(r)
	err := d.Decode(&fsm.distMap)
	if err != nil {
		return err
	}
	return nil
}

type snapshot struct {
	distMap map[string]any
	brokers map[int]Broker
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	encoder := gob.NewEncoder(sink)
	err := encoder.Encode(s.distMap)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("sink.Write(): %v", err)
	}
	return sink.Close()
}

func (s *snapshot) Release() {

}
