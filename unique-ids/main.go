package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var sequence = 1

func main() {
	n := maelstrom.NewNode()
	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "generate_ok"
		body["id"] = generateID(n.ID(), sequence)

		sequence++

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func generateID(nodeID string, sequence int) string {
	return fmt.Sprintf("%s-%d-%d", nodeID, time.Now().UnixNano(), sequence)
}
