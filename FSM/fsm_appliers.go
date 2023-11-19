package FSM

import (
	"YAKT/helpers"
	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"strconv"
)

func (fsm *DistMap) ApplyBrokerCreate(l *raft.Log) interface{} {
	broker := Broker{}
	err := helpers.GobDecode[Broker](l.Data, &broker)
	if err != nil {
		return ApplyRv{
			MetaData: map[string]any{"status": "ERROR"},
			Error:    err,
		}
	}
	broker.internalUUID = uuid.New().String()
	broker.Epoch = 0
	fsm.Brokers[broker.BrokerID] = broker

	return ApplyRv{
		MetaData: map[string]any{
			"status": "SUCCESS",
			"broker": broker},
		Error: nil,
	}
}

func (fsm *DistMap) ApplyBrokerDelete(l *raft.Log) interface{} {
	brokerID, _ := strconv.Atoi(string(l.Data)) //caller ensures valid brokerID

	delete(fsm.Brokers, brokerID)
	return nil
}

func (fsm *DistMap) ApplyBrokerReplace(l *raft.Log) interface{} {
	broker := Broker{}
	err := helpers.GobDecode[Broker](l.Data, &broker)
	if err != nil {
		return ApplyRv{
			MetaData: map[string]any{"status": "ERROR"},
			Error:    err,
		}
	}

	oldBroker := fsm.Brokers[broker.BrokerID] //caller ensures brokerID is valid and exists
	broker.internalUUID = oldBroker.internalUUID
	broker.Epoch = oldBroker.Epoch + 1
	fsm.Brokers[oldBroker.BrokerID] = broker

	return ApplyRv{
		MetaData: map[string]any{
			"status":   "SUCCESS",
			"brokerID": strconv.Itoa(broker.BrokerID)},
		Error: nil,
	}
}

func (fsm *DistMap) ApplyTopicCreate(l *raft.Log) interface{} {
	topicName := string(l.Data)
	newTopic := Topic{
		Name:      topicName,
		topicUUID: uuid.New().String(),
	}
	fsm.Topics.TopicMap[topicName] = newTopic
	fsm.Topics.Offset += 1

	return fsm.Topics.Offset
}

func (fsm *DistMap) ApplyPartitionCreate(l *raft.Log) interface{} {

	newPartition := Partition{}
	err := helpers.GobDecode[Partition](l.Data, &newPartition)
	if err != nil {
		return ApplyRv{
			MetaData: map[string]any{"status": "ERROR"},
			Error:    err,
		}
	}
	fsm.Partitions.PartitionMap[newPartition.PartitionID] = newPartition

	return ApplyRv{
		MetaData: map[string]interface{}{
			"status":    "SUCCESS",
			"partition": newPartition,
		},
		Error: nil,
	}
}
