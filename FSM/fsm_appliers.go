package FSM

import (
	"YAKT/helpers"
	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"log"
	"strconv"
	"strings"
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
	broker.LogicalTime = fsm.LogicalClock + 1
	broker.BrokerStatus = "Active"
	fsm.Brokers.BrokerMap[broker.BrokerID] = broker
	log.Printf("[INFO][BROKER][CREATE] Created Broker %+v\n", broker)

	fsm.LogicalClock++
	return ApplyRv{
		MetaData: map[string]any{
			"status": "SUCCESS",
			"broker": broker},
		Error: nil,
	}
}

func (fsm *DistMap) ApplyBrokerDelete(l *raft.Log) interface{} {
	brokerID, _ := strconv.Atoi(string(l.Data)) //caller ensures valid brokerID
	if len(fsm.Brokers.DeletedBrokers) > MAX_TIME_OFFSET {
		fsm.Brokers.DeletedBrokers = fsm.Brokers.DeletedBrokers[1:]
	}
	tbdBroker := fsm.Brokers.BrokerMap[brokerID]
	tbdBroker.LogicalTime = fsm.LogicalClock + 1
	fsm.Brokers.DeletedBrokers = append(fsm.Brokers.DeletedBrokers, tbdBroker)

	delete(fsm.Brokers.BrokerMap, brokerID)
	log.Printf("[INFO][BROKER][DELETE] Deleted Broker with brokerID %d\n", brokerID)
	fsm.LogicalClock++
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

	oldBroker := fsm.Brokers.BrokerMap[broker.BrokerID] //caller ensures brokerID is valid and exists
	broker.internalUUID = oldBroker.internalUUID
	broker.Epoch = oldBroker.Epoch + 1
	broker.LogicalTime = fsm.LogicalClock + 1
	fsm.Brokers.BrokerMap[oldBroker.BrokerID] = broker
	log.Printf("[INFO][BROKER][REPLACE] Replace Broker %+v with %+v\n", oldBroker, broker)

	fsm.LogicalClock++
	return ApplyRv{
		MetaData: map[string]any{
			"status": "SUCCESS",
			"broker": broker,
		},
		Error: nil,
	}
}

func (fsm *DistMap) ApplyTopicCreate(l *raft.Log) interface{} {
	topicName := string(l.Data)
	newTopic := Topic{
		Name:        topicName,
		topicUUID:   uuid.New().String(),
		LogicalTime: fsm.LogicalClock + 1,
	}
	fsm.Topics.TopicMap[topicName] = newTopic
	log.Printf("[INFO][TOPIC][CREATE] Create Topic %s", topicName)

	fsm.LogicalClock++
	return fsm.LogicalClock
}

func (fsm *DistMap) ApplyProducerCreate(l *raft.Log) interface{} {
	data := string(l.Data)
	res := strings.Split(data, "|")
	brokerId, err := strconv.Atoi(res[0]) //caller ensure valid and existent brokerId
	if err != nil {
		return ApplyRv{
			MetaData: map[string]any{"status": "ERROR"},
			Error:    err,
		}
	}
	producerId, err := strconv.Atoi(res[1]) //caller ensures valid producerId
	if err != nil {
		return ApplyRv{
			MetaData: map[string]any{"status": "ERROR"},
			Error:    err,
		}
	}

	//Get brokerUUID from brokerId
	brokerUUID := fsm.Brokers.BrokerMap[brokerId].internalUUID
	brokerEpoch := fsm.Brokers.BrokerMap[brokerId].Epoch

	producer := Producer{
		brokerUUID,
		brokerEpoch,
		producerId,
		fsm.LogicalClock + 1,
	}

	fsm.Producers = append(fsm.Producers, producer)
	log.Printf("[INFO][PRODUCER][CREATE] Create ProducerIdsRecord{producerId: %d, brokerId: %d} = %+v\n", producerId, brokerId, producer)

	fsm.LogicalClock++ //incrementing logical time
	return ApplyRv{
		MetaData: map[string]any{
			"status": "SUCCESS",
		},
		Error: nil,
	}
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
	leaderId, _ := strconv.Atoi(newPartition.Leader) //caller ensures leader is valid BrokerId

	//newPartition.TopicUUID from req contains topic name as bound by req
	newPartition.TopicUUID = fsm.Topics.TopicMap[newPartition.TopicUUID].topicUUID
	newPartition.Leader = fsm.Brokers.BrokerMap[leaderId].internalUUID
	newPartition.PartitionEpoch = 0

	newPartition.LogicalTime = fsm.LogicalClock + 1
	fsm.Partitions.PartitionMap[newPartition.PartitionID] = newPartition

	fsm.LogicalClock++
	return ApplyRv{
		MetaData: map[string]interface{}{
			"status":    "SUCCESS",
			"partition": newPartition,
		},
		Error: nil,
	}
}
