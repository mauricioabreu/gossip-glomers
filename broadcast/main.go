package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type reqTopologyBody struct {
	Topology map[string][]string `json:"topology"`
}

type reqBroadcastBody struct {
	Message float64 `json:"message"`
}

func main() {
	messages := make([]float64, 0)
	neighbors := make([]string, 0)
	n := maelstrom.NewNode()
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body reqBroadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messages = append(messages, body.Message)

		for _, neighbor := range neighbors {
			if neighbor != msg.Src {
				for _, message := range messages {
					gBody := map[string]any{"type": "broadcast", "message": message}
					n.RPC(neighbor, gBody, func(gmsg maelstrom.Message) error { return nil })
				}
			}
		}

		return n.Reply(msg, map[string]string{"type": "broadcast_ok"})
	})
	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = messages

		return n.Reply(msg, body)
	})
	n.Handle("topology", func(msg maelstrom.Message) error {
		var body reqTopologyBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		response := make(map[string]string)
		response["type"] = "topology_ok"
		neighbors = body.Topology[n.ID()]

		return n.Reply(msg, map[string]string{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
