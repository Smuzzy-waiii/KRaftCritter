package FSM

import (
	"encoding/gob"
	"fmt"
	"github.com/hashicorp/raft"
	"io"
	"log"
)

// DistMap impl.  raft.FSM
type DistMap struct {
	Brokers map[int]Broker
	Topics  Topics
}

func (fsm *DistMap) InitIfNotInit() {
	if (*fsm).Brokers == nil {
		(*fsm).Brokers = make(map[int]Broker)
	}

	if (*fsm).Topics.TopicMap == nil {
		(*fsm).Topics.TopicMap = make(map[string]Topic)
		(*fsm).Topics.Offset = 0
	}
}

type ApplyRv struct {
	MetaData map[string]any
	Error    error
}

func (fsm *DistMap) Apply(l *raft.Log) interface{} {
	fsm.InitIfNotInit()

	logType := string(l.Extensions)
	switch logType {

	case "Broker":
		return fsm.ApplyBrokerCreate(l)

	case "DeleteBroker":
		return fsm.ApplyBrokerDelete(l)

	case "ReplaceBroker":
		return fsm.ApplyBrokerReplace(l)

	case "Topic":
		return fsm.ApplyTopicCreate(l)
	}
	log.Fatalln("Log type not recognised")
	return ApplyRv{}
}

func (fsm *DistMap) Snapshot() (raft.FSMSnapshot, error) {
	brokerCopy := make(map[int]Broker)
	for k := range fsm.Brokers {
		brokerCopy[k] = fsm.Brokers[k]
	}
	return &snapshot{
		brokerCopy,
	}, nil
}

func (fsm *DistMap) Restore(r io.ReadCloser) error {
	d := gob.NewDecoder(r)
	err := d.Decode(&fsm)
	if err != nil {
		return err
	}
	return nil
}

type snapshot struct {
	Brokers map[int]Broker
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	encoder := gob.NewEncoder(sink)
	err := encoder.Encode(s)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("sink.Write(): %v", err)
	}
	return sink.Close()
}

func (s *snapshot) Release() {

}
