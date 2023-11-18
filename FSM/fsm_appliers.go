package FSM

import (
	"YAKT/helpers"
	"fmt"
	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"strconv"
	"strings"
)

func (fsm *DistMap) ApplyKVStore(l *raft.Log) interface{} {
	data := string(l.Data)
	res := strings.Split(data, "|")
	key := res[0]
	val := res[1]
	fmt.Printf("[INFO][KV-STORE] Setting key = %s | value = %s\n", key, val)
	(*fsm).DistMap[key] = val
	//currently setting value to nil making it a hashtable
	return nil
}

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
