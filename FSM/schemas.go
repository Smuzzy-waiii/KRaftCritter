package FSM

type Broker struct {
	internalUUID     string
	BrokerID         int    `json:"brokerID"`
	BrokerHost       string `json:"brokerHost"`
	BrokerPort       string `json:"brokerPort"`
	SecurityProtocol string `json:"securityProtocol"`
	BrokerStatus     string `json:"brokerStatus"`
	RackID           string `json:"rackID"`
	Epoch            int    `json:"epoch"`
}

type Topic struct {
	topicUUID string
	Name      string `json:"name"`
}

type Partition struct {
	PartitionID      int    `json:"partitionId"`
	TopicUUID        string `json:"topicName"`
	Replicas         []int  `json:"replicas"`
	ISR              []int  `json:"ISR"`
	RemovingReplicas []int  `json:"removingReplicas"`
	AddingReplicas   []int  `json:"addingReplicas"`
	Leader           string `json:"leader"`
	PartitionEpoch   int    `json:"partitionEpoch"`
}

type ProducerID struct {
	brokerID    string
	brokerEpoch int
	producerID  int
}

type Topics struct {
	TopicMap map[string]Topic
	Offset   int
}

type Partitions struct {
	PartitionMap map[int]Partition
}

//TODO : partitions request gob encode decode this only
// same as partition
//leader id ( instead of leader)
//topic id (instead of topic uuid)
//do not include epoch
