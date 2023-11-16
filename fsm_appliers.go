package main

import (
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
	fmt.Printf("[INFO] Setting key = %s | value = %s\n", key, val)
	(*fsm).distMap[key] = val
	//currently setting value to nil making it a hashtable
	return nil
}

func (fsm *DistMap) ApplyBroker(l *raft.Log) interface{} {
	broker := Broker{}
	err := gobDecode[Broker](l.Data, &broker)
	if err != nil {
		return ApplyRv{
			MetaData: map[string]string{"status": "ERROR"},
			Error:    err,
		}
	}
	broker.internalUUID = uuid.New().String()
	broker.epoch = 0
	fsm.brokers[broker.BrokerID] = broker
	return ApplyRv{
		MetaData: map[string]string{
			"status":   "SUCCESS",
			"brokerID": strconv.Itoa(broker.BrokerID)},
		Error: nil,
	}
}
