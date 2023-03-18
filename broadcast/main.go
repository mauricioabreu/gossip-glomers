package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type reqTopologyBody struct {
	Topology map[string][]string `json:"topology"`
}

type reqBroadcastBody struct {
	Message float64 `json:"message"`
}

func main() {
	var mutex sync.RWMutex
	messages := make(map[float64]bool)
	neighbors := make([]string, 0)

	n := maelstrom.NewNode()
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		mutex.Lock()
		defer mutex.Unlock()

		var body reqBroadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if messages[body.Message] {
			return nil
		} else {
			messages[body.Message] = true
		}

		for _, neighbor := range neighbors {
			if neighbor != msg.Src {
				go doRPC(n, neighbor, body.Message)
			}
		}

		return n.Reply(msg, map[string]string{"type": "broadcast_ok"})
	})
	n.Handle("read", func(msg maelstrom.Message) error {
		replyMsgs := make([]float64, 0)
		for m := range messages {
			replyMsgs = append(replyMsgs, m)
		}

		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": replyMsgs})
	})
	n.Handle("topology", func(msg maelstrom.Message) error {
		var body reqTopologyBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		neighbors = body.Topology[n.ID()]

		return n.Reply(msg, map[string]string{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func doRPC(n *maelstrom.Node, dest string, message float64) {
	retryAfter := 100 * time.Millisecond
	alreadyReceived := false
	body := map[string]any{"type": "broadcast", "message": message}
	err := n.RPC(dest, body, func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if body["type"] == "broadcast_ok" {
			alreadyReceived = true
		}

		return nil
	})

	time.Sleep(retryAfter)
	if !alreadyReceived || err != nil {
		doRPC(n, dest, message)
	}
}
