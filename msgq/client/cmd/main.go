package main

import (
	"fmt"

	infraclient "github.com/mizosoft/graft/infra/client"
	"github.com/mizosoft/graft/msgq/client"
)

func main() {
	infraclient.RunClient("msgq-cli", "CLI for the message queue service",
		infraclient.CmdWithArgs("enqueue", "<topic> <data>", "Add a message to a topic", func(ctx *infraclient.Context) error {
			args := ctx.Args()
			if len(args) < 2 {
				return fmt.Errorf("usage: enqueue <topic> <data>")
			}
			mq := client.NewMsgqClient(ctx.ClientId, ctx.Config)
			id, err := mq.Enqueue(args[0], args[1])
			if err != nil {
				return err
			}
			fmt.Printf("OK (id: %s)\n", id)
			return nil
		}),

		infraclient.CmdWithArgs("dequeue", "<topic>", "Get and remove a message from a topic", func(ctx *infraclient.Context) error {
			args := ctx.Args()
			if len(args) < 1 {
				return fmt.Errorf("missing topic argument")
			}
			mq := client.NewMsgqClient(ctx.ClientId, ctx.Config)
			msg, ok, err := mq.Dequeue(args[0])
			if err != nil {
				return err
			}
			if !ok {
				fmt.Println("(empty)")
			} else {
				fmt.Printf("id: %s\ndata: %s\n", msg.Id, msg.Data)
			}
			return nil
		}),
	)
}
