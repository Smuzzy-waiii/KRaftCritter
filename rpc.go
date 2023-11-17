package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/raft"
	"log"
	"net/http"
	"strconv"
	"time"
)

type rpcInterface struct {
	fsm  *DistMap
	raft *raft.Raft
}

func (r rpcInterface) getAll(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"data":        r.fsm.DistMap,
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

	val, prs := r.fsm.DistMap[key]
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

	serData := []byte(fmt.Sprint(key, "|", val))
	f := r.raft.ApplyLog(
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

func (r rpcInterface) RegisterBroker(c *gin.Context) {
	broker := new(Broker)
	if !BindMiddleware(c, broker) {
		return
	}

	if !r.CheckBrokerIdExistsInFSM(c, broker.BrokerID, false) {
		return
	}

	serBroker, err := gobEncode(broker) // serialized broker
	if !HandleEncodingError(c, err) {
		return
	}

	f := r.raft.ApplyLog(raft.Log{Data: serBroker, Extensions: []byte("Broker")}, time.Second)
	if err := f.Error(); !HandleApplyError(c, err) {
		return
	}

	resp := f.Response().(ApplyRv)
	if err := resp.Error; !HandleApplyRvError(c, err) {
		return
	}

	strconv.Atoi(resp.MetaData["brokerID"])

	c.JSON(http.StatusOK, gin.H{
		"status":      "SUCCESS",
		"message":     "Broker Created Successfully",
		"brokerID":    broker.BrokerID,
		"commitIndex": f.Index(),
	})
}

func (r rpcInterface) ReplaceBroker(c *gin.Context) {
	broker := Broker{BrokerID: -1}
	if !BindMiddleware(c, &broker) {
		return
	}

	brokerId := broker.BrokerID
	if !CheckBrokerIdParamExists(c, brokerId == -1) {
		return
	}

	if !CheckAllBrokerFieldsExist(c, broker) { //Does not check BrokerID
		return
	}

	if !r.CheckBrokerIdExistsInFSM(c, brokerId, true) {
		return
	}

	serBroker, err := gobEncode(broker) // serialized broker
	if !HandleEncodingError(c, err) {
		return
	}

	f := r.raft.ApplyLog(raft.Log{Data: serBroker, Extensions: []byte("ReplaceBroker")}, time.Second)
	if err := f.Error(); !HandleApplyError(c, err) {
		return
	}

	resp := f.Response().(ApplyRv)
	if err := resp.Error; !HandleApplyError(c, err) {
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":      "SUCCESS",
		"message":     "Broker Replaced Successfully",
		"broker":      broker,
		"commitIndex": f.Index(),
	})
}

func (r rpcInterface) DeleteBroker(c *gin.Context) {
	brokerId := c.DefaultQuery("brokerID", "")
	if !CheckBrokerIdParamExists(c, brokerId == "") {
		return
	}

	brokerIdInt, err := strconv.Atoi(brokerId)
	if !HandleAtoiError(c, err) {
		return
	}

	if !r.CheckBrokerIdExistsInFSM(c, brokerIdInt, true) {
		return
	}

	serBrokerId := []byte(brokerId)

	f := r.raft.ApplyLog(raft.Log{Data: serBrokerId, Extensions: []byte("DeleteBroker")}, time.Second)
	if err := f.Error(); !HandleApplyError(c, err) {
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":      "SUCCESS",
		"message":     "Broker Deleted Successfully",
		"commitIndex": f.Index(),
	})
}

func (r rpcInterface) GetBrokers(c *gin.Context) {
	brokerId := c.DefaultQuery("brokerID", "")
	if brokerId == "" {
		var brokers []Broker
		for brokerId := range r.fsm.Brokers {
			broker := r.fsm.Brokers[brokerId]
			brokers = append(brokers, broker)
		}
		c.JSON(http.StatusOK, gin.H{
			"status":  "SUCCESS",
			"brokers": brokers,
		})
	} else {
		brokerIdInt, err := strconv.Atoi(brokerId)
		if !HandleAtoiError(c, err) {
			return
		}

		if !r.CheckBrokerIdExistsInFSM(c, brokerIdInt, true) {
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status": "SUCCESS",
			"broker": r.fsm.Brokers[brokerIdInt],
		})
	}
}

func (r rpcInterface) GetAllActiveBrokers(c *gin.Context) {
	var brokers []Broker
	for brokerId := range r.fsm.Brokers {
		if broker := r.fsm.Brokers[brokerId]; broker.BrokerStatus == "Active" {
			brokers = append(brokers, broker)
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"status":        "SUCCESS",
		"activeBrokers": brokers,
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
