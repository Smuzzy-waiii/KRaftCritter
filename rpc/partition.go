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

func (r RpcInterface) CreatePartition(c *gin.Context) {

	partition := FSM.Partition{PartitionID: -1}
	if !BindMiddleware(c, partition) {
		return
	}

	if !CheckParamExists(c, partition.PartitionID == -1, "partitionID") {
		return
	}

	if !CheckParamExists(c, partition.Leader == "", "leader") {
		return
	}

	if !CheckParamExists(c, partition.TopicUUID == "", "topicName") {
		return
	}

	if !r.CheckPartitionExistsInFSM(c, partition.PartitionID, false) {
		return
	}

	//partition.TopicUUID contains topicname
	if !r.CheckTopicExistsInFSM(c, partition.TopicUUID, true) {
		return
	}

	//partition.leader container brokerId in string form
	leaderIdInt, err := strconv.Atoi(partition.Leader)
	if !HandleGenericAtoiError(c, err, "leader") {
		return
	}

	if !r.CheckBrokerIdExistsInFSM(c, leaderIdInt, true) {
		return
	}

	serPartition, err := helpers.GobEncode(partition) //serialize partition
	if !HandleEncodingError(c, err) {
		return
	}

	// Apply the log to Raft node
	f := r.Raft.ApplyLog(raft.Log{Data: serPartition, Extensions: []byte("Partition")}, time.Second)
	if err := f.Error(); !HandleApplyError(c, err) {
		return
	}

	resp := f.Response().(FSM.ApplyRv)
	if err := resp.Error; !HandleApplyRvError(c, err) {
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":      "SUCCESS",
		"message":     "Partition Created Successfully",
		"partition":   resp.MetaData["partition"].(FSM.Partition),
		"commitIndex": f.Index(),
	})
}

// UNUSED COZ REQS CHANGED -_-
// TODO: Impl. FSM Applier for this
func (r RpcInterface) RemoveReplica(c *gin.Context) {
	partitionID := c.Query("partitionID")
	brokerID := c.Query("brokerID")

	if partitionID == "" || brokerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "Bad Request",
			"message": "Both partitionID and/or brokerID isnt given",
		})
		return
	}

	// Use GobEncode to serialize the data
	encodedData, err := helpers.GobEncode([]string{partitionID, brokerID})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "Error",
			"message": "Failed to encode data",
		})
		return
	}

	f := r.Raft.ApplyLog(raft.Log{
		Data:       encodedData,
		Extensions: []byte("RemoveReplica"),
	}, time.Second)

	if err := f.Error(); !HandleApplyError(c, err) {
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":      "SUCCESS",
		"message":     "Replica Removed Successfully",
		"partitionID": partitionID,
		"brokerID":    brokerID,
		"commitIndex": f.Index(),
	})
}

// UNUSED COZ REQS CHANGED -_-
// TODO: Impl. FSM Applier for this
func (r RpcInterface) AddReplica(c *gin.Context) {
	partitionID := c.Query("partitionID")
	replicaID := c.Query("replicaID")

	if partitionID == "" || replicaID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "Bad Request",
			"message": "Both partitionID and replicaID are required parameters",
		})
		return
	}

	// Use GobEncode to serialize the data
	encodedData, err := helpers.GobEncode([]string{partitionID, replicaID})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "Error",
			"message": "Failed to encode data",
		})
		return
	}

	// Apply the log to Raft node for adding replica
	f := r.Raft.ApplyLog(raft.Log{
		Data:       encodedData,
		Extensions: []byte("AddReplica"),
	}, time.Second)

	if err := f.Error(); !HandleApplyError(c, err) {
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":      "SUCCESS",
		"message":     "Replica Added Successfully",
		"partitionID": partitionID,
		"replicaID":   replicaID,
		"commitIndex": f.Index(),
	})
}
