package rpc

import (
	"YAKT/FSM"
	"YAKT/helpers"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/raft"
	"log"
	"net/http"
)

type RpcInterface struct {
	Fsm  *FSM.DistMap
	Raft *raft.Raft
}

func (r RpcInterface) RedirectLeader() gin.HandlerFunc {
	return func(c *gin.Context) {
		curr_leader_serv_addr, _ := r.Raft.LeaderWithID()
		curr_leader_addr := string(curr_leader_serv_addr)

		if curr_leader_addr == "" {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"message": "Leader Currently Unavailable. Retry after some time.",
			})
			c.Abort()
			return
		}

		leaderHost, leaderHttpPort, err := helpers.GetHttpAddrFromGrpcAddr(curr_leader_addr)
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
