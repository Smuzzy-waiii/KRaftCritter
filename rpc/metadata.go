package rpc

import (
	"YAKT/FSM"
	"YAKT/helpers"
	"github.com/gin-gonic/gin"
	"net/http"
	"strconv"
)

func (r RpcInterface) GetClientMetadata(c *gin.Context) {
	prevLogicalTime := c.DefaultQuery("prevLogicalTime", "")
	if prevLogicalTime == "" {
		r.GetAllClientMetadata(c)
		return
	} else {
		prevLogicalTimeInt, err := strconv.Atoi(prevLogicalTime)
		if !HandleTimeAtoiError(c, err) {
			return
		}

		if r.Fsm.LogicalClock-prevLogicalTimeInt > FSM.MAX_TIME_OFFSET {
			r.GetAllClientMetadata(c)
			return
		} else {
			r.GetDiffClientMetadata(c, prevLogicalTimeInt)
			return
		}
	}
}

func (r RpcInterface) GetAllClientMetadata(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":      "SUCCESS",
		"logicalTime": r.Fsm.LogicalClock,
		"brokers":     helpers.Values(r.Fsm.Brokers.BrokerMap),
		"topics":      helpers.Values(r.Fsm.Topics.TopicMap),
		"partitions":  helpers.Values(r.Fsm.Partitions.PartitionMap),
	})
}

func (r RpcInterface) GetDiffClientMetadata(c *gin.Context, prevLogicalTime int) {
	brokers := []FSM.Broker{}
	topics := []FSM.Topic{}
	partitions := []FSM.Partition{}

	for _, broker := range r.Fsm.Brokers.BrokerMap {
		if broker.LogicalTime > prevLogicalTime {
			brokers = append(brokers, broker)
		}
	}

	for _, topic := range r.Fsm.Topics.TopicMap {
		if topic.LogicalTime > prevLogicalTime {
			topics = append(topics, topic)
		}
	}

	for _, partition := range r.Fsm.Partitions.PartitionMap {
		if partition.LogicalTime > prevLogicalTime {
			partitions = append(partitions, partition)
		}
	}

	deletedBrokers := []FSM.Broker{}
	for _, broker := range r.Fsm.Brokers.DeletedBrokers {
		if broker.LogicalTime > prevLogicalTime {
			deletedBrokers = append(deletedBrokers, broker)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"status":      "SUCCESS",
		"logicalTime": r.Fsm.LogicalClock,
		"brokers": gin.H{
			"upserted": brokers,
			"deleted":  deletedBrokers,
		},
		"topics": gin.H{
			"upserted": topics,
			"deleted":  []FSM.Topics{},
		},
		"partitions": gin.H{
			"upserted": partitions,
			"deleted":  []FSM.Partition{},
		},
	})
}

func (r RpcInterface) GetBrokerMetadata(c *gin.Context) {
	prevLogicalTime := c.DefaultQuery("prevLogicalTime", "")
	if prevLogicalTime == "" {
		r.GetAllBrokerMetadata(c)
		return
	} else {
		prevLogicalTimeInt, err := strconv.Atoi(prevLogicalTime)
		if !HandleTimeAtoiError(c, err) {
			return
		}

		if r.Fsm.LogicalClock-prevLogicalTimeInt > FSM.MAX_TIME_OFFSET {
			r.GetAllBrokerMetadata(c)
			return
		} else {
			r.GetDiffBrokerMetadata(c, prevLogicalTimeInt)
			return
		}
	}
}

func (r RpcInterface) GetAllBrokerMetadata(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":      "SUCCESS",
		"logicalTime": r.Fsm.LogicalClock,
		"brokers":     helpers.Values(r.Fsm.Brokers.BrokerMap),
		"topics":      helpers.Values(r.Fsm.Topics.TopicMap),
		"producers":   r.Fsm.Producers,
		"partitions":  helpers.Values(r.Fsm.Partitions.PartitionMap),
	})
}

func (r RpcInterface) GetDiffBrokerMetadata(c *gin.Context, prevLogicalTime int) {
	brokers := []FSM.Broker{}
	topics := []FSM.Topic{}
	producers := []FSM.Producer{}
	partitions := []FSM.Partition{}

	for _, broker := range r.Fsm.Brokers.BrokerMap {
		if broker.LogicalTime > prevLogicalTime {
			brokers = append(brokers, broker)
		}
	}

	for _, topic := range r.Fsm.Topics.TopicMap {
		if topic.LogicalTime > prevLogicalTime {
			topics = append(topics, topic)
		}
	}

	for _, producer := range r.Fsm.Producers {
		if producer.LogicalTime > prevLogicalTime {
			producers = append(producers, producer)
		}
	}

	for _, partition := range r.Fsm.Partitions.PartitionMap {
		if partition.LogicalTime > prevLogicalTime {
			partitions = append(partitions, partition)
		}
	}

	deletedBrokers := []FSM.Broker{}
	for _, broker := range r.Fsm.Brokers.DeletedBrokers {
		if broker.LogicalTime > prevLogicalTime {
			deletedBrokers = append(deletedBrokers, broker)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"status":      "SUCCESS",
		"logicalTime": r.Fsm.LogicalClock,
		"brokers": gin.H{
			"upserted": brokers,
			"deleted":  deletedBrokers,
		},
		"topics": gin.H{
			"upserted": topics,
			"deleted":  []FSM.Topics{},
		},
		"producers": gin.H{
			"upserted": producers,
			"deleted":  []FSM.Producer{},
		},
		"partitions": gin.H{
			"upserted": partitions,
			"deleted":  []FSM.Partition{},
		},
	})
}
