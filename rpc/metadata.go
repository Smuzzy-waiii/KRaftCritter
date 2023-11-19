package rpc

import (
	"github.com/gin-gonic/gin"
	"strconv"
)

func (r RpcInterface) GetClientMetadata(c *gin.Context) {
	prevLogicalTime := c.DefaultQuery("prevLogicalTime", "")
	if !CheckParamExists(c, prevLogicalTime == "", "prevLogicalTime") {
		return
	}

	prevLogicalTimeInt, err := strconv.Atoi(prevLogicalTime)
	if !HandleTimeAtoiError(c, err) {
		return
	}

	result := make(map[string]any)
	//

}
