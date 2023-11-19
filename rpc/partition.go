package rpc

import (
	"YAKT/FSM"
	"YAKT/helpers"
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/raft"
	"net/http"
	"time"
)

func (r RpcInterface) CreatePartition(c *gin.Context) {
	partition := new(FSM.Partition)
	if !BindMiddleware(c, partition) {
		return
	}

	if !r.CheckPartitionExistsInFSM(c, partition.PartitionID, false) {
		return
	}

	serPartition, err := helpers.GobEncode(partition)
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

func (r RpcInterface) RemoveReplica(c *gin.Context) {
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

	// Apply the log to Raft node for removing replica
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
		"replicaID":   replicaID,
		"commitIndex": f.Index(),
	})
}

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

//func (r RpcInterface) CreatePartition(c *gin.Context) {
//	partition := new(FSM.Partition)
//	if !BindMiddleware(c, partition) {
//		return
//	}
//	partitionID := c.Query("partitionId")
//	topicUUID := c.Query("topicUUID")
//	replicas := parseIntArrayQueryParam(c, "replicas")
//
//	if !CheckParamExists(c, partitionID == "", "partitionId") ||
//		!CheckParamExists(c, topicUUID == "", "topicUUID") ||
//		!CheckParamExists(c, replicas == nil, "replicas") {
//		return
//	}
////	f:= r.Raft.ApplyLog(raft.Log{Data: [[]byte(partitionID),[]byte(topicUUID),[]byte(replicas)]}), Extensions : []byte("Partition")},time.Second)
////	if err := f.Error(); !HandleApplyError(c, err) {
////return
////
////}
//	f := r.Raft.ApplyLog(raft.Log{
//		Data:       serializePartition(partitionID, topicUUID, replicas),
//		Extensions: []byte("Partition"),
//	}, time.Second)
//
//	if err := f.Error(); err != nil {
//		// Handle the error, e.g., log it or return an error response
//		c.JSON(http.StatusInternalServerError, gin.H{
//			"status":  "Error",
//			"message": "Failed to apply log to Raft node",
//			"error":   err.Error(),
//		})
//		return
//	}
//offset := f.Response().(int)
//
//c.JSON(http.StatusOK, gin.H{
//"status":      "SUCCESS",
//"message":     "Partition Created Successfully",
//"partition":   Partition{PartitionID: partitionID, TopicUUID: topicUUID, Replicas: replicas},
//"offset":      offset,
//"commitIndex": f.Index(),
//})
//}
//
//
//}
//////////////////
//
//// Y
//
//

//func serializePartition(partitionID, topicUUID string, replicas []int) []byte {
//	// Implement your serialization logic here, e.g., using encoding/json
//	// For simplicity, assuming a JSON serialization here
//	partition := Partition{
//		PartitionID:      // convert partitionID to int,
//		TopicUUID:        topicUUID,
//		Replicas:         replicas,
//		ISR:              []int{},
//		RemovingReplicas: []int{},
//		AddingReplicas:   []int{},
//		Leader:           "",
//		PartitionEpoch:   0,
//	}
//
//	data, _ := json.Marshal(partition)
//	return data
//}
//

//func parseIntArrayQueryParam(c *gin.Context, param string) []int {
//	paramValue := c.QueryArray(param)
//	var result []int
//	for _, val := range paramValue {
//		intVal, err := strconv.Atoi(val)
//		if err == nil {
//			result = append(result, intVal)
//		}
//	}
//	return result
//}
//
