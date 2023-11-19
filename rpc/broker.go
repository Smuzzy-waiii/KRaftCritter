package rpc

import (
	"YAKT/FSM"
	"YAKT/helpers"
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/raft"
	"net/http"
	"strconv"
	"time"
)

func (r RpcInterface) RegisterBroker(c *gin.Context) {
	broker := new(FSM.Broker)
	if !BindMiddleware(c, broker) {
		return
	}

	if !r.CheckBrokerIdExistsInFSM(c, broker.BrokerID, false) {
		return
	}

	serBroker, err := helpers.GobEncode(broker) // serialized broker
	if !HandleEncodingError(c, err) {
		return
	}

	f := r.Raft.ApplyLog(raft.Log{Data: serBroker, Extensions: []byte("Broker")}, time.Second)
	if err := f.Error(); !HandleApplyError(c, err) {
		return
	}

	resp := f.Response().(FSM.ApplyRv)
	if err := resp.Error; !HandleApplyRvError(c, err) {
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":      "SUCCESS",
		"message":     "Broker Created Successfully",
		"broker":      resp.MetaData["broker"].(FSM.Broker),
		"commitIndex": f.Index(),
	})
}

func (r RpcInterface) ReplaceBroker(c *gin.Context) {
	broker := FSM.Broker{BrokerID: -1}
	if !BindMiddleware(c, &broker) {
		return
	}

	brokerId := broker.BrokerID
	if !CheckParamExists(c, brokerId == -1, "brokerID") {
		return
	}

	if !CheckAllBrokerFieldsExist(c, broker) { //Does not check BrokerID
		return
	}

	if !r.CheckBrokerIdExistsInFSM(c, brokerId, true) {
		return
	}

	serBroker, err := helpers.GobEncode(broker) // serialized broker
	if !HandleEncodingError(c, err) {
		return
	}

	f := r.Raft.ApplyLog(raft.Log{Data: serBroker, Extensions: []byte("ReplaceBroker")}, time.Second)
	if err := f.Error(); !HandleApplyError(c, err) {
		return
	}

	resp := f.Response().(FSM.ApplyRv)
	if err := resp.Error; !HandleApplyError(c, err) {
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":      "SUCCESS",
		"message":     "Broker Replaced Successfully",
		"broker":      broker,
		"commitIndex": f.Index(),
	})
}

//TODO: Add PatchBroker Endpoint

func (r RpcInterface) DeleteBroker(c *gin.Context) {
	brokerId := c.DefaultQuery("brokerID", "")
	if !CheckParamExists(c, brokerId == "", "brokerID") {
		return
	}

	brokerIdInt, err := strconv.Atoi(brokerId)
	if !HandleBrokerIdAtoiError(c, err) {
		return
	}

	if !r.CheckBrokerIdExistsInFSM(c, brokerIdInt, true) {
		return
	}

	serBrokerId := []byte(brokerId)

	f := r.Raft.ApplyLog(raft.Log{Data: serBrokerId, Extensions: []byte("DeleteBroker")}, time.Second)
	if err := f.Error(); !HandleApplyError(c, err) {
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":      "SUCCESS",
		"message":     "Broker Deleted Successfully",
		"commitIndex": f.Index(),
	})
}

func (r RpcInterface) GetBrokers(c *gin.Context) {
	brokerId := c.DefaultQuery("brokerID", "")
	if brokerId == "" {
		var brokers []FSM.Broker
		for brokerId := range r.Fsm.Brokers {
			broker := r.Fsm.Brokers[brokerId]
			brokers = append(brokers, broker)
		}
		c.JSON(http.StatusOK, gin.H{
			"status":  "SUCCESS",
			"brokers": brokers,
		})
	} else {
		brokerIdInt, err := strconv.Atoi(brokerId)
		if !HandleBrokerIdAtoiError(c, err) {
			return
		}

		if !r.CheckBrokerIdExistsInFSM(c, brokerIdInt, true) {
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status": "SUCCESS",
			"broker": r.Fsm.Brokers[brokerIdInt],
		})
	}
}

func (r RpcInterface) GetAllActiveBrokers(c *gin.Context) {
	var brokers []FSM.Broker
	for brokerId := range r.Fsm.Brokers {
		if broker := r.Fsm.Brokers[brokerId]; broker.BrokerStatus == "Active" {
			brokers = append(brokers, broker)
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"status":        "SUCCESS",
		"activeBrokers": brokers,
	})
}
