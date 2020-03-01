package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/dependencies/filesystem"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/repl"
	"github.com/influxdata/flux/runtime"
	_ "github.com/influxdata/flux/stdlib"
	"github.com/influxdata/flux/stdlib/influxdata/influxdb"
	_ "github.com/influxdata/influxdb/query/stdlib"
	"github.com/spf13/cobra"
)

var replFlags struct {
	org organization
}

func cmdREPL() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "repl",
		Short: "Interactive Flux REPL (read-eval-print-loop)",
		Args:  cobra.NoArgs,
		RunE:  wrapCheckSetup(replF),
	}
	replFlags.org.register(cmd, false)

	return cmd
}

func replF(cmd *cobra.Command, args []string) error {
	if flags.local {
		return fmt.Errorf("local flag not supported for repl command")
	}

	if err := replFlags.org.validOrgFlags(); err != nil {
		return err
	}

	plan.RegisterLogicalRules(
		influxdb.DefaultFromAttributes{
			Org: &influxdb.NameOrID{
				ID:   replFlags.org.id,
				Name: replFlags.org.name,
			},
			Host:  &flags.host,
			Token: &flags.token,
		},
	)
	runtime.FinalizeBuiltIns()

	r, err := getFluxREPL(flags.skipVerify)
	if err != nil {
		return err
	}

	r.Run()
	return nil
}

func getFluxREPL(skipVerify bool) (*repl.REPL, error) {
	deps := flux.NewDefaultDependencies()
	deps.Deps.FilesystemService = filesystem.SystemFS
	if skipVerify {
		deps.Deps.HTTPClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		}
	}
	ctx := deps.Inject(context.Background())
	return repl.New(ctx, deps), nil
}
