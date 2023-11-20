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
	LogicalTime      int
}

type Topic struct {
	topicUUID   string
	Name        string `json:"name"`
	LogicalTime int
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
	LogicalTime      int
}

type Producer struct {
	BrokerUUID  string
	BrokerEpoch int
	ProducerID  int
	LogicalTime int
}

type Brokers struct {
	BrokerMap      map[int]Broker
	DeletedBrokers []Broker
}

type Topics struct {
	TopicMap map[string]Topic
	Offset   int
}

type Partitions struct {
	PartitionMap map[int]Partition
}
