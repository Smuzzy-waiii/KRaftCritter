package rpc

import (
	"YAKT/FSM"
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/raft"
	"net/http"
	"time"
)

func (r RpcInterface) CreateTopic(c *gin.Context) {
	topicName := c.DefaultQuery("name", "")
	if !CheckParamExists(c, topicName == "", "name") {
		return
	}

	if !r.CheckTopicExistsInFSM(c, topicName, false) {
		return
	}

	f := r.Raft.ApplyLog(raft.Log{Data: []byte(topicName), Extensions: []byte("Topic")}, time.Second)
	if err := f.Error(); !HandleApplyError(c, err) {
		return
	}

	offset := f.Response().(int)

	c.JSON(http.StatusOK, gin.H{
		"status":      "SUCCESS",
		"message":     "Topic Created Successfully",
		"name":        topicName,
		"offset":      offset,
		"commitIndex": f.Index(),
	})
}

func (r RpcInterface) GetTopics(c *gin.Context) {
	topicName := c.DefaultQuery("name", "")
	if topicName == "" {
		var topics []FSM.Topic
		for topicName := range r.Fsm.Topics.TopicMap {
			topic := r.Fsm.Topics.TopicMap[topicName]
			topics = append(topics, topic)
		}
		c.JSON(http.StatusOK, gin.H{
			"status": "SUCCESS",
			"topics": topics,
		})
	} else {
		if !r.CheckTopicExistsInFSM(c, topicName, true) {
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status": "SUCCESS",
			"topic":  r.Fsm.Topics.TopicMap[topicName],
		})
	}
}
