package rpc

import (
	"YAKT/FSM"
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
)

func BindMiddleware(c *gin.Context, obj interface{}) bool {
	err := c.Bind(obj)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
			"type":  "BindError",
		})
		return false
	}
	return true
}

func HandleEncodingError(c *gin.Context, err error) bool {
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
			"type":  "EncodingError",
		})
		return false
	}
	return true
}

func HandleApplyError(c *gin.Context, err error) bool {
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
			"type":  "ApplyError",
		})
		return false
	}
	return true
}

func HandleApplyRvError(c *gin.Context, err error) bool {
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
			"type":  "ApplyRvError",
		})
		return false
	}
	return true
}

func (r RpcInterface) CheckBrokerIdExistsInFSM(c *gin.Context, brokerID int, shouldExist bool) bool {
	_, prs := r.Fsm.Brokers[brokerID]
	if !shouldExist && prs {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "AlreadyExists",
			"message": "Broker with same brokerID already exists",
		})
		return false
	} else if shouldExist && !prs {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "BrokerDoesNotExist",
			"message": fmt.Sprintf("Broker with brokerID %d does not exist", brokerID),
		})
		return false
	} else {
		return true
	}
}

func CheckBrokerIdParamExists(c *gin.Context, doesNotExist bool) bool {
	if doesNotExist {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Specify brokerID",
		})
		return false
	}
	return true
}

func HandleAtoiError(c *gin.Context, err error) bool {
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status": "InvalidBrokerId",
			"error":  err,
		})
		return false
	}
	return true
}

func CheckAllBrokerFieldsExist(c *gin.Context, broker FSM.Broker) bool {
	switch "" {
	case broker.BrokerStatus, broker.BrokerHost, broker.BrokerPort, broker.SecurityProtocol, broker.SecurityProtocol:
		c.JSON(http.StatusBadRequest, gin.H{
			"status":         "IncompleteBroker",
			"message":        "All Broker fields required",
			"recievedBroker": broker,
			"brokerSchema":   FSM.Broker{},
		})
		return false
	}
	return true
}
