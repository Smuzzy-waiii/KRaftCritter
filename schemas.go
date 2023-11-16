package main

type Broker struct {
	internalUUID     string
	BrokerID         int    `json:"brokerID"`
	BrokerHost       string `json:"brokerHost"`
	BrokerPort       string `json:"brokerPort"`
	SecurityProtocol string `json:"securityProtocol"`
	BrokerStatus     string `json:"brokerStatus"`
	RackID           string `json:"rackID"`
	epoch            int
}

type Topic struct {
	topicUUID string
	name      string
}

type Partition struct {
	partitionID      int
	topicUUID        string
	replicas         []int
	ISR              []int
	removingReplicas []int
	addingReplicas   []int
	leader           string
	partitionEpoch   int
}

type ProducerID struct {
	brokerID    string
	brokerEpoch int
	producerID  int
}
