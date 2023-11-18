package rpc

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/raft"
	"net/http"
	"time"
)

func (r RpcInterface) GetAll(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"data":        r.Fsm.DistMap,
		"readAtIndex": r.Raft.AppliedIndex(),
	})
}

func (r RpcInterface) GetValue(c *gin.Context) {
	key := c.DefaultQuery("key", "")
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":       "Specify Key",
			"readAtIndex": r.Raft.AppliedIndex(),
		})
		return
	}

	val, prs := r.Fsm.DistMap[key]
	if !prs {
		c.JSON(http.StatusOK, gin.H{
			"status":      "ERR_NO_KEY",
			"message":     "Key not present",
			"key":         key,
			"value":       nil,
			"readAtIndex": r.Raft.AppliedIndex(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":      "SUCCESS",
		"message":     "Key found!",
		"key":         key,
		"value":       val,
		"readAtIndex": r.Raft.AppliedIndex(),
	})
}

func (r RpcInterface) SetValue(c *gin.Context) {
	key := c.DefaultQuery("key", "")
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Specify Key",
		})
		return
	}

	val := c.DefaultQuery("value", "")
	if val == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Specify Value",
		})
		return
	}

	serData := []byte(fmt.Sprint(key, "|", val))
	f := r.Raft.ApplyLog(
		raft.Log{
			Data:       serData,
			Extensions: []byte("KeyValue"),
		},
		time.Second)
	if err := f.Error(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"status":      "SUCCESS",
		"message":     "Value successfully stored",
		"key":         key,
		"value":       val,
		"commitIndex": f.Index(),
	})
}
