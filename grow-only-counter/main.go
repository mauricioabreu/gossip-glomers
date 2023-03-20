package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type reqAddMessage struct {
	Delta int `json:"delta"`
}

type Counter struct {
	n   *maelstrom.Node
	kv  *maelstrom.KV
	key string
}

func NewCounter(n *maelstrom.Node, key string) *Counter {
	return &Counter{
		n:   n,
		kv:  maelstrom.NewSeqKV(n),
		key: key,
	}
}

func (c *Counter) Converge(delta int) error {
	for {
		ctx := context.Background()
		value, err := c.kv.ReadInt(ctx, c.key)
		if err != nil {
			value = 0
		}

		if err := c.kv.CompareAndSwap(ctx, c.key, value, value+delta, true); err == nil {
			return nil
		}
	}
}

func (c *Counter) Read() (int, error) {
	value, err := c.kv.ReadInt(context.Background(), c.key)
	if err != nil {
		return 0, err
	}

	return value, nil
}

func main() {
	n := maelstrom.NewNode()
	counter := NewCounter(n, "g-counter")

	n.Handle("add", func(msg maelstrom.Message) error {
		var body reqAddMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if err := counter.Converge(body.Delta); err != nil {
			return err
		}
		return n.Reply(msg, map[string]string{"type": "add_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		value, err := counter.Read()
		if err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": value,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
