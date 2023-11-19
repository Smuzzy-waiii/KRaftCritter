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
	partitionID      int
	topicUUID        string
	replicas         []int
	ISR              []int
	removingReplicas []int
	addingReplicas   []int
	leader           string
	partitionEpoch   int
}

type Producer struct {
	BrokerUUID  string
	BrokerEpoch int
	ProducerID  int
	LogicalTime int
}

type Topics struct {
	TopicMap map[string]Topic
	Offset   int
}
