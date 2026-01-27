package client

import (
	"fmt"
	"os"

	"github.com/mizosoft/graft/infra"
	"github.com/urfave/cli/v2"
)

// Context provides access to the client configuration within command handlers.
type Context struct {
	ClientId string
	Config   map[string]string
	Client   *Client
	cliCtx   *cli.Context
}

// Args returns the non-flag arguments for the current command.
func (c *Context) Args() []string {
	return c.cliCtx.Args().Slice()
}

// Cmd creates a new command for use with RunClient.
func Cmd(name, usage string, action func(ctx *Context) error) *cli.Command {
	return &cli.Command{
		Name:  name,
		Usage: usage,
		Action: func(c *cli.Context) error {
			ctx, err := newContext(c)
			if err != nil {
				return err
			}
			return action(ctx)
		},
	}
}

// CmdWithArgs creates a command that shows argument usage in help.
func CmdWithArgs(name, argsUsage, usage string, action func(ctx *Context) error) *cli.Command {
	return &cli.Command{
		Name:      name,
		Usage:     usage,
		ArgsUsage: argsUsage,
		Action: func(c *cli.Context) error {
			ctx, err := newContext(c)
			if err != nil {
				return err
			}
			return action(ctx)
		},
	}
}

func newContext(c *cli.Context) (*Context, error) {
	configFile := c.String("config")
	clientId := c.String("id")

	config, err := infra.ParseConfigFile(configFile)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	return &Context{
		ClientId: clientId,
		Config:   config,
		Client:   New(clientId, config),
		cliCtx:   c,
	}, nil
}

// RunClient runs a graft client CLI with subcommands.
//
// Usage:
//
//	func main() {
//	    client.RunClient("kvstore-cli", "CLI for kvstore",
//	        client.CmdWithArgs("get", "<key>", "Get a value by key", func(ctx *client.Context) error {
//	            args := ctx.Args()
//	            if len(args) < 1 {
//	                return fmt.Errorf("missing key argument")
//	            }
//	            // Use ctx.Client to make requests...
//	            return nil
//	        }),
//	        client.CmdWithArgs("put", "<key> <value>", "Set a value", func(ctx *client.Context) error {
//	            // ...
//	        }),
//	    )
//	}
func RunClient(name, usage string, commands ...*cli.Command) {
	app := &cli.App{
		Name:  name,
		Usage: usage,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "config",
				Value: "config.txt",
				Usage: "Path to cluster config file",
			},
			&cli.StringFlag{
				Name:  "id",
				Value: "client",
				Usage: "Client ID",
			},
		},
		Commands: commands,
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
