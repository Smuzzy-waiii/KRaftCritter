package rpc

import (
	"YAKT/FSM"
	"github.com/gin-gonic/gin"
)

func (r RpcInterface) CreatePartition(c *gin.Context) {
	partition := new(FSM.Partition)
	if !BindMiddleware(c, partition) {
		return
	}

}
