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
	broker.LogicalTime = fsm.logicalClock + 1
	fsm.Brokers[broker.BrokerID] = broker
	log.Printf("[INFO][BROKER][CREATE] Created Broker %+v\n", broker)

	fsm.logicalClock++
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
	log.Printf("[INFO][BROKER][DELETE] Deleted Broker with brokerID %d\n", brokerID)
	fsm.logicalClock++
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
	broker.LogicalTime = fsm.logicalClock + 1
	fsm.Brokers[oldBroker.BrokerID] = broker
	log.Printf("[INFO][BROKER][REPLACE] Replace Broker %+v with %+v\n", oldBroker, broker)

	fsm.logicalClock++
	return ApplyRv{
		MetaData: map[string]any{
			"status":   "SUCCESS",
			"brokerID": strconv.Itoa(broker.BrokerID),
		},
		Error: nil,
	}
}

func (fsm *DistMap) ApplyTopicCreate(l *raft.Log) interface{} {
	topicName := string(l.Data)
	newTopic := Topic{
		Name:        topicName,
		topicUUID:   uuid.New().String(),
		LogicalTime: fsm.logicalClock + 1,
	}
	fsm.Topics.TopicMap[topicName] = newTopic
	fsm.Topics.Offset += 1
	log.Printf("[INFO][TOPIC][CREATE] Create Topic %s", topicName)

	fsm.logicalClock++
	return fsm.logicalClock
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
	brokerUUID := fsm.Brokers[brokerId].internalUUID
	brokerEpoch := fsm.Brokers[brokerId].Epoch

	producer := Producer{
		brokerUUID,
		brokerEpoch,
		producerId,
		fsm.logicalClock + 1,
	}

	fsm.Producers = append(fsm.Producers, producer)
	log.Printf("[INFO][PRODUCER][CREATE] Create ProducerIdsRecord{producerId: %d, brokerId: %d} = %+v\n", producerId, brokerId, producer)

	fsm.logicalClock++ //incrementing logical time
	return ApplyRv{
		MetaData: map[string]any{
			"status": "SUCCESS",
		},
		Error: nil,
	}
}
