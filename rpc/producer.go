package rpc

import (
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/raft"
	"net/http"
	"strconv"
	"time"
)

func (r RpcInterface) RegisterProducer(c *gin.Context) {
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

	producerId := c.DefaultQuery("producerID", "")
	if !CheckParamExists(c, producerId == "", "producerID") {
		return
	}

	_, err = strconv.Atoi(producerId)
	if !HandleProducerIdAtoiError(c, err) {
		return
	}

	serData := []byte(brokerId + "|" + producerId)

	f := r.Raft.ApplyLog(raft.Log{Data: serData, Extensions: []byte("Producer")}, time.Second)
	if err := f.Error(); !HandleApplyError(c, err) {
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":      "SUCCESS",
		"message":     "Producer Created Successfully",
		"commitIndex": f.Index(),
	})
}
