// Copyright 2025 The NATS Authors
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
	"context"
	"fmt"
	"math/big"
	"os"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/natscli/columns"
	iu "github.com/nats-io/natscli/internal/util"
	"github.com/synadia-io/orbit.go/counters"

	"github.com/spf13/cobra"
)

type counterCmd struct {
	subject string
	stream  string
	json    bool
}

func configureCounterCommand(app commandHost) {
	c := counterCmd{}

	ctr := addCommand(app, "counter", "Access distributed Counters")
	ctr.Aliases = []string{"ctr"}
	ctr.Long = "This is an experimental command and will undergo changes in later versions"

	get := addCommand(ctr, "get", "Gets the current value for a counter")
	get.Aliases = []string{"g"}
	get.RunE = c.getAction
	cmdAddTags(get, "scope:user", "impact:ro")
	addArg(get, "subject", "Subject to get counter for", true, "string")
	get.Flags().StringVar(&c.stream, "stream", "", "The stream name to fetch the value from")

	view := addCommand(ctr, "view", "View the full counter metadata")
	view.Aliases = []string{"v"}
	view.RunE = c.viewAction
	cmdAddTags(view, "scope:user", "impact:ro")
	addArg(view, "subject", "Subject to get counter for", true, "string")
	view.Flags().StringVar(&c.stream, "stream", "", "The stream name to fetch the value from")

	incr := addCommand(ctr, "increment", "Increment the value of a counter")
	incr.Aliases = []string{"incr", "inc", "i"}
	incr.RunE = c.incrAction
	cmdAddTags(incr, "scope:user", "impact:rw")
	addArg(incr, "subject", "Subject to get counter for", true, "string")
	addArg(incr, "value", "The value to increment", true, "string")
	incr.Flags().StringVar(&c.stream, "stream", "", "The stream name to fetch the value from")

	decr := addCommand(ctr, "decrement", "Decrement the value of a counter")
	decr.Aliases = []string{"decr", "dec", "d"}
	decr.RunE = c.incrAction
	cmdAddTags(decr, "scope:user", "impact:rw")
	addArg(decr, "subject", "Subject to get counter for", true, "string")
	addArg(decr, "value", "The value to decrement", true, "string")
	decr.Flags().StringVar(&c.stream, "stream", "", "The stream name to fetch the value from")

	ls := addCommand(ctr, "list", "List Counters in a stream")
	ls.Aliases = []string{"l", "ls"}
	ls.RunE = c.lsAction
	cmdAddTags(ls, "scope:user", "impact:ro")
	addArg(ls, "subject", "Subject pattern to get counters for", false, "string")
	ls.Flags().StringVar(&c.stream, "stream", "", "The stream name to fetch the value from")
	ls.Flags().BoolVar(&c.json, "json", false, "Produce JSON output")
}

func init() {
	registerCommand("counter", 5, configureCounterCommand)
}

func (c *counterCmd) lsAction(_ *cobra.Command, args []string) error {
	c.subject = argValue(args, 0)

	if c.stream == "" && c.subject == "" {
		return fmt.Errorf("no stream or subject specified")
	}

	stream, _, err := c.getCounterManager()
	if err != nil {
		return err
	}

	if c.subject == "" {
		c.subject = ">"
	}

	nfo, err := stream.Info(context.Background(), jetstream.WithSubjectFilter(c.subject))
	if err != nil {
		return err
	}

	if len(nfo.State.Subjects) == 0 {
		return fmt.Errorf("no counters found")
	}

	var subjects []string
	for k := range nfo.State.Subjects {
		subjects = append(subjects, k)
	}

	if c.json {
		return iu.PrintJSON(subjects)
	}

	for _, subject := range subjects {
		fmt.Println(subject)
	}

	return nil
}

func (c *counterCmd) getAction(_ *cobra.Command, args []string) error {
	c.subject = args[0]

	_, ctr, err := c.getCounterManager()
	if err != nil {
		return err
	}

	val, err := ctr.Load(context.Background(), c.subject)
	if err != nil {
		return err
	}

	fmt.Println(val.String())

	return nil
}

func (c *counterCmd) incrAction(cmd *cobra.Command, args []string) error {
	c.subject = args[0]

	_, ctr, err := c.getCounterManager()
	if err != nil {
		return err
	}

	v := &big.Int{}
	_, ok := v.SetString(argValue(args, 1), 10)
	if !ok {
		return fmt.Errorf("invalid value")
	}

	if cmd.Name() == "decrement" {
		v.Neg(v)
	}

	val, err := ctr.Add(context.Background(), c.subject, v)
	if err != nil {
		return err
	}

	fmt.Println(val.String())

	return nil
}

func (c *counterCmd) viewAction(_ *cobra.Command, args []string) error {
	c.subject = args[0]

	_, ctr, err := c.getCounterManager()
	if err != nil {
		return err
	}

	val, err := ctr.Get(context.Background(), c.subject)
	if err != nil {
		return err
	}

	cols := columns.Newf("Counter %v", c.subject)

	cols.AddRow("Value", val.Value)
	cols.AddRow("Increment", val.Incr)
	cols.AddRow("Subject", val.Subject)

	if len(val.Sources) > 0 {
		cols.AddSectionTitle("Sources")
		for s, v := range val.Sources {
			vals := map[string]string{}
			for k, incr := range v {
				vals[k] = incr.String()
			}

			cols.AddMapStringsAsValue(s, vals)
		}
	} else {
		cols.Println("No source information found")
	}

	return cols.Frender(os.Stdout)
}

func (c *counterCmd) getCounterManager() (jetstream.Stream, counters.Counter, error) {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return nil, nil, err
	}

	js, err := newJetStreamWithOptions(nc, opts())
	if err != nil {
		return nil, nil, err
	}

	name := c.stream
	if name == "" {
		name, err = js.StreamNameBySubject(context.Background(), c.subject)
		if err != nil {
			return nil, nil, err
		}
	}

	stream, err := js.Stream(context.Background(), name)
	if err != nil {
		return nil, nil, err
	}

	cfg := stream.CachedInfo().Config

	if !(cfg.AllowMsgCounter && cfg.AllowDirect) {
		return nil, nil, fmt.Errorf("%q is not a valid counter stream", name)
	}

	ctr, err := counters.NewCounterFromStream(js, stream)
	if err != nil {
		return nil, nil, err
	}

	return stream, ctr, nil
}
