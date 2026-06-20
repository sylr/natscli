// Copyright 2023-2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cli

import (
	"fmt"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/natscli/top"
	"github.com/spf13/cobra"
	ui "gopkg.in/gizak/termui.v1"
)

type topCmd struct {
	host            string
	conns           int
	delay           int
	sort            string
	lookup          bool
	output          string
	outputDelimiter string
	raw             bool
	maxRefresh      int
	showSubs        bool
}

func configureTopCommand(app commandHost) {
	c := &topCmd{}

	top := addCommand(app, "top", "Shows top-like statistic for connections on a specific server")
	top.RunE = c.topAction
	addArg(top, "name", "The server name to gather statistics for", true, "string")
	top.Flags().IntVarP(&c.conns, "conns", "n", 1024, "Maximum number of connections to show")
	top.Flags().IntVarP(&c.delay, "interval", "d", 1, "Refresh interval")
	top.Flags().Var(newEnumValue(&c.sort, "cid", "cid", "start", "subs", "pending", "msgs_to", "msgs_from", "bytes_to", "bytes_from", "last", "idle", "uptime", "stop", "reason", "rtt"), "sort", "Sort connections by")
	top.Flags().BoolVar(&c.lookup, "lookup", false, "Looks up client addresses in DNS")
	top.Flags().StringVarP(&c.output, "output", "o", "", "Saves the first snapshot to a file")
	top.Flags().StringVar(&c.outputDelimiter, "delimiter", "", "Specifies a output delimiter, defaults to grid-like text")
	top.Flags().BoolVarP(&c.raw, "raw", "b", false, "Show raw bytes")
	top.Flags().IntVarP(&c.maxRefresh, "max-refresh", "r", -1, "Maximum refreshes")
	top.Flags().BoolVar(&c.showSubs, "subs", false, "Shows the subscriptions column")
}

func init() {
	registerCommand("top", 17, configureTopCommand)
}

func (c *topCmd) topAction(_ *cobra.Command, args []string) error {
	c.host = args[0]

	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	engine := top.NewEngine(nc, c.host, c.conns, c.delay, opts().Trace)

	_, err = engine.Request("VARZ")
	if err != nil {
		return fmt.Errorf("initial test request failed: %v", err)
	}

	sortOpt := server.SortOpt(c.sort)
	if !sortOpt.IsValid() {
		return fmt.Errorf("invalid sort option: %s", c.sort)
	}
	engine.SortOpt = sortOpt
	engine.DisplaySubs = c.showSubs

	if c.output != "" {
		return top.SaveStatsSnapshotToFile(engine, c.output, c.outputDelimiter)
	}

	err = ui.Init()
	if err != nil {
		panic(err)
	}
	defer ui.Close()

	go engine.MonitorStats()

	top.StartUI(engine, c.lookup, c.raw, c.maxRefresh)

	return nil
}
