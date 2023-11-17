package main

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

func (r rpcInterface) TestEndpoint(c *gin.Context) {
	broker := Broker{BrokerID: -1}
	err := c.Bind(&broker)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
			"type":  "BindError",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"broker": broker,
	})
}
