package FSM

import (
	"YAKT/helpers"
	"encoding/gob"
	"fmt"
	"github.com/hashicorp/raft"
	"io"
	"log"
)

const MAX_TIME_OFFSET = 25

// DistMap impl.  raft.FSM
type DistMap struct {
	LogicalClock int
	Brokers      Brokers
	Topics       Topics
	Producers    []Producer
	Partitions   Partitions
}

func (fsm *DistMap) InitIfNotInit() {
	if (*fsm).Brokers.BrokerMap == nil {
		(*fsm).Brokers.BrokerMap = make(map[int]Broker)
	}

	if (*fsm).Topics.TopicMap == nil {
		(*fsm).Topics.TopicMap = make(map[string]Topic)
		(*fsm).Topics.Offset = 0
	}
	if (*fsm).Partitions.PartitionMap == nil {
		(*fsm).Partitions.PartitionMap = make(map[int]Partition)
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

	case "Partition":
		return fsm.ApplyPartitionCreate(l)

	case "Producer":
		return fsm.ApplyProducerCreate(l)

	}
	log.Fatalln("Log type not recognised")
	return ApplyRv{}
}

func (fsm *DistMap) Snapshot() (raft.FSMSnapshot, error) {
	brokerMapCopy := make(map[int]Broker)
	helpers.DeepCopyMap(&brokerMapCopy, fsm.Brokers.BrokerMap)

	deletedBrokersCopy := []Broker{}
	helpers.DeepCopySlice(&deletedBrokersCopy, fsm.Brokers.DeletedBrokers)

	topicMapCopy := make(map[string]Topic)
	helpers.DeepCopyMap(&topicMapCopy, fsm.Topics.TopicMap)

	partitionMapCopy := make(map[int]Partition)
	helpers.DeepCopyMap(&partitionMapCopy, fsm.Partitions.PartitionMap)

	producerCopy := []Producer{}
	helpers.DeepCopySlice(&producerCopy, fsm.Producers)

	return &snapshot{
		LogicalClock: fsm.LogicalClock,
		Brokers: Brokers{
			BrokerMap:      brokerMapCopy,
			DeletedBrokers: deletedBrokersCopy,
		},
		Topics: Topics{
			TopicMap: topicMapCopy,
			Offset:   fsm.Topics.Offset,
		},
		Producers: producerCopy,
		Partitions: Partitions{
			PartitionMap: partitionMapCopy,
		},
	}, nil
}

func (fsm *DistMap) Restore(r io.ReadCloser) error {
	restoreSnapshot := snapshot{}
	d := gob.NewDecoder(r)
	err := d.Decode(&restoreSnapshot)
	if err != nil {
		return err
	}

	(*fsm) = DistMap{
		LogicalClock: restoreSnapshot.LogicalClock,
		Brokers: Brokers{
			restoreSnapshot.BrokerMap,
			restoreSnapshot.DeletedBrokers,
		},
		Topics: Topics{
			TopicMap: restoreSnapshot.TopicMap,
			Offset:   restoreSnapshot.Offset,
		},
		Producers: restoreSnapshot.Producers,
		Partitions: Partitions{
			restoreSnapshot.PartitionMap,
		},
	}
	return nil
}

type snapshot struct {
	LogicalClock int
	Brokers
	Topics
	Partitions
	Producers []Producer
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
