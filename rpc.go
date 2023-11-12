package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/raft"
	"log"
	"net/http"
	"time"
)

type rpcInterface struct {
	fsm  *DistMap
	raft *raft.Raft
}

func (r rpcInterface) getAll(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"data":        r.fsm.distMap,
		"readAtIndex": r.raft.AppliedIndex(),
	})
}

func (r rpcInterface) getValue(c *gin.Context) {
	key := c.DefaultQuery("key", "")
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":       "Specify Key",
			"readAtIndex": r.raft.AppliedIndex(),
		})
		return
	}

	val, prs := r.fsm.distMap[key]
	if !prs {
		c.JSON(http.StatusOK, gin.H{
			"status":      "ERR_NO_KEY",
			"message":     "Key not present",
			"key":         key,
			"value":       nil,
			"readAtIndex": r.raft.AppliedIndex(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":      "SUCCESS",
		"message":     "Key found!",
		"key":         key,
		"value":       val,
		"readAtIndex": r.raft.AppliedIndex(),
	})
}

func (r rpcInterface) setValue(c *gin.Context) {
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

	f := r.raft.Apply([]byte(fmt.Sprint(key, "|", val)), time.Second)
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

func (r rpcInterface) RedirectLeader() gin.HandlerFunc {
	return func(c *gin.Context) {
		curr_leader_serv_addr, _ := r.raft.LeaderWithID()
		curr_leader_addr := string(curr_leader_serv_addr)

		if curr_leader_addr == "" {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"message": "Leader Currently Unavailable. Retry after some time.",
			})
			c.Abort()
			return
		}

		leaderHost, leaderHttpPort, err := getHttpAddrFromGrpcAddr(curr_leader_addr)
		if err != nil {
			log.Fatalf("failed to parse leader address (%q): %v", curr_leader_addr, err)
		}
		curr_leader_http_addr := leaderHost + ":" + leaderHttpPort

		if curr_leader_http_addr != c.Request.Host {
			redirect_url := "http://" + curr_leader_http_addr + c.Request.RequestURI
			fmt.Printf("[WARN] Request {%s} to Non-Leader, Redirecting to leader {%s}\n", c.Request.Host+c.Request.URL.String(), redirect_url)
			c.Redirect(http.StatusPermanentRedirect, redirect_url)
			c.Abort()
		} else {
			c.Next()
		}
	}
}
