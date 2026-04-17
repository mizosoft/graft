package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/mizosoft/graft/dlock/client"
	infraclient "github.com/mizosoft/graft/infra/client"
)

func main() {
	infraclient.RunClient("dlock-cli", "CLI for the distributed lock service",
		infraclient.CmdWithArgs("lock", "<resource> <ttl-ms>", "Acquire a write lock", func(ctx *infraclient.Context) error {
			args := ctx.Args()
			if len(args) < 2 {
				return fmt.Errorf("usage: lock <resource> <ttl-ms>")
			}
			ttl, err := strconv.ParseInt(args[1], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid ttl: %v", err)
			}
			dl := client.NewDlockClient(ctx.ClientId, ctx.Config)
			token, ok, err := dl.TryLock(args[0], time.Duration(ttl)*time.Millisecond)
			if err != nil {
				return err
			}
			if ok {
				fmt.Printf("OK (token: %d)\n", token)
			} else {
				fmt.Println("FAILED (resource locked)")
			}
			return nil
		}),

		infraclient.CmdWithArgs("rlock", "<resource> <ttl-ms>", "Acquire a read lock", func(ctx *infraclient.Context) error {
			args := ctx.Args()
			if len(args) < 2 {
				return fmt.Errorf("usage: rlock <resource> <ttl-ms>")
			}
			ttl, err := strconv.ParseInt(args[1], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid ttl: %v", err)
			}
			dl := client.NewDlockClient(ctx.ClientId, ctx.Config)
			token, ok, err := dl.TryRLock(args[0], time.Duration(ttl)*time.Millisecond)
			if err != nil {
				return err
			}
			if ok {
				fmt.Printf("OK (token: %d)\n", token)
			} else {
				fmt.Println("FAILED (resource locked)")
			}
			return nil
		}),

		infraclient.CmdWithArgs("unlock", "<resource> <token>", "Release a write lock", func(ctx *infraclient.Context) error {
			args := ctx.Args()
			if len(args) < 2 {
				return fmt.Errorf("usage: unlock <resource> <token>")
			}
			token, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid token: %v", err)
			}
			dl := client.NewDlockClient(ctx.ClientId, ctx.Config)
			ok, err := dl.Unlock(args[0], token)
			if err != nil {
				return err
			}
			if ok {
				fmt.Println("OK")
			} else {
				fmt.Println("FAILED (invalid token or not locked)")
			}
			return nil
		}),

		infraclient.CmdWithArgs("runlock", "<resource> <token>", "Release a read lock", func(ctx *infraclient.Context) error {
			args := ctx.Args()
			if len(args) < 2 {
				return fmt.Errorf("usage: runlock <resource> <token>")
			}
			token, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid token: %v", err)
			}
			dl := client.NewDlockClient(ctx.ClientId, ctx.Config)
			ok, err := dl.RUnlock(args[0], token)
			if err != nil {
				return err
			}
			if ok {
				fmt.Println("OK")
			} else {
				fmt.Println("FAILED (invalid token or not locked)")
			}
			return nil
		}),

		infraclient.CmdWithArgs("refresh", "<resource> <token> <ttl-ms>", "Refresh a write lock TTL", func(ctx *infraclient.Context) error {
			args := ctx.Args()
			if len(args) < 3 {
				return fmt.Errorf("usage: refresh <resource> <token> <ttl-ms>")
			}
			token, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid token: %v", err)
			}
			ttl, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid ttl: %v", err)
			}
			dl := client.NewDlockClient(ctx.ClientId, ctx.Config)
			ok, err := dl.RefreshLock(args[0], token, time.Duration(ttl)*time.Millisecond)
			if err != nil {
				return err
			}
			if ok {
				fmt.Println("OK")
			} else {
				fmt.Println("FAILED (invalid token or expired)")
			}
			return nil
		}),

		infraclient.CmdWithArgs("rrefresh", "<resource> <token> <ttl-ms>", "Refresh a read lock TTL", func(ctx *infraclient.Context) error {
			args := ctx.Args()
			if len(args) < 3 {
				return fmt.Errorf("usage: rrefresh <resource> <token> <ttl-ms>")
			}
			token, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid token: %v", err)
			}
			ttl, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid ttl: %v", err)
			}
			dl := client.NewDlockClient(ctx.ClientId, ctx.Config)
			ok, err := dl.RefreshRLock(args[0], token, time.Duration(ttl)*time.Millisecond)
			if err != nil {
				return err
			}
			if ok {
				fmt.Println("OK")
			} else {
				fmt.Println("FAILED (invalid token or expired)")
			}
			return nil
		}),
	)
}
