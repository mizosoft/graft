package main

import (
	"fmt"

	infraclient "github.com/mizosoft/graft/infra/client"
	"github.com/mizosoft/graft/kvstore/client"
)

func main() {
	infraclient.RunClient("kvstore-cli", "CLI for the kvstore service",
		infraclient.CmdWithArgs("get", "<key>", "Get a value by key", func(ctx *infraclient.Context) error {
			args := ctx.Args()
			if len(args) < 1 {
				return fmt.Errorf("missing key argument")
			}
			kv := client.NewKvClient(ctx.ClientId, ctx.Config)
			val, exists, err := kv.Get(args[0])
			if err != nil {
				return err
			}
			if !exists {
				fmt.Println("(nil)")
			} else {
				fmt.Println(val)
			}
			return nil
		}),

		infraclient.CmdWithArgs("put", "<key> <value>", "Set a key to a value", func(ctx *infraclient.Context) error {
			args := ctx.Args()
			if len(args) < 2 {
				return fmt.Errorf("usage: put <key> <value>")
			}
			kv := client.NewKvClient(ctx.ClientId, ctx.Config)
			prev, existed, err := kv.Put(args[0], args[1])
			if err != nil {
				return err
			}
			if existed {
				fmt.Printf("OK (previous: %s)\n", prev)
			} else {
				fmt.Println("OK")
			}
			return nil
		}),

		infraclient.CmdWithArgs("delete", "<key>", "Delete a key", func(ctx *infraclient.Context) error {
			args := ctx.Args()
			if len(args) < 1 {
				return fmt.Errorf("missing key argument")
			}
			kv := client.NewKvClient(ctx.ClientId, ctx.Config)
			val, existed, err := kv.Delete(args[0])
			if err != nil {
				return err
			}
			if existed {
				fmt.Printf("Deleted: %s\n", val)
			} else {
				fmt.Println("(nil)")
			}
			return nil
		}),

		infraclient.CmdWithArgs("cas", "<key> <expected> <new>", "Compare and swap", func(ctx *infraclient.Context) error {
			args := ctx.Args()
			if len(args) < 3 {
				return fmt.Errorf("usage: cas <key> <expected> <new>")
			}
			kv := client.NewKvClient(ctx.ClientId, ctx.Config)
			ok, curr, err := kv.Cas(args[0], args[1], args[2])
			if err != nil {
				return err
			}
			if ok {
				fmt.Println("OK")
			} else {
				fmt.Printf("FAILED (current: %s)\n", curr)
			}
			return nil
		}),

		infraclient.CmdWithArgs("append", "<key> <value>", "Append to a value", func(ctx *infraclient.Context) error {
			args := ctx.Args()
			if len(args) < 2 {
				return fmt.Errorf("usage: append <key> <value>")
			}
			kv := client.NewKvClient(ctx.ClientId, ctx.Config)
			length, err := kv.Append(args[0], args[1])
			if err != nil {
				return err
			}
			fmt.Printf("OK (length: %d)\n", length)
			return nil
		}),

		infraclient.CmdWithArgs("putIfAbsent", "<key> <value>", "Put only if key doesn't exist", func(ctx *infraclient.Context) error {
			args := ctx.Args()
			if len(args) < 2 {
				return fmt.Errorf("usage: putIfAbsent <key> <value>")
			}
			kv := client.NewKvClient(ctx.ClientId, ctx.Config)
			ok, err := kv.PutIfAbsent(args[0], args[1])
			if err != nil {
				return err
			}
			if ok {
				fmt.Println("OK")
			} else {
				fmt.Println("FAILED (key exists)")
			}
			return nil
		}),
	)
}
