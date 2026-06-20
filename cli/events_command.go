// Copyright 2020-2025 The NATS Authors
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
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/spf13/cobra"
)

type eventsCmd struct {
	json  bool
	ce    bool
	short bool

	bodyF   string
	bodyFRe *regexp.Regexp

	showJsMetrics        bool
	showJsAdvisories     bool
	showServerAdvisories bool
	showAll              bool
	extraSubjects        []string
	stream               string
	since                time.Duration

	sync.Mutex
}

func configureEventsCommand(app commandHost) {
	c := &eventsCmd{}

	events := addCommand(app, "events", "Show Advisories and Events")
	events.Aliases = []string{"event", "e"}
	events.RunE = c.eventsAction
	cmdAddTags(events, "scope:user", "impact:ro")
	addCheat("events", events)
	events.Flags().BoolVarP(&c.showAll, "all", "a", false, "Show all events")
	events.Flags().BoolVarP(&c.json, "json", "j", false, "Produce JSON output")
	events.Flags().BoolVar(&c.ce, "cloudevent", false, "Produce CloudEvents v1 output")
	events.Flags().BoolVar(&c.short, "short", false, "Short event format")
	events.Flags().StringVar(&c.bodyF, "filter", ".", "Filter across the entire event using regular expressions")
	events.Flags().BoolVar(&c.showJsMetrics, "js-metric", false, "Shows JetStream metric events (false)")
	events.Flags().BoolVar(&c.showJsAdvisories, "js-advisory", false, "Shows advisory events (false)")
	negatableBoolVar(events, &c.showServerAdvisories, "srv-advisory", true, "Shows NATS Server advisories (true)")
	events.Flags().StringArrayVar(&c.extraSubjects, "subjects", nil, "Show Advisories and Metrics received on specific subjects")
	flagPlaceholder(events, "subjects", "SUBJECTS")
	events.Flags().StringVar(&c.stream, "stream", "", "Reads events from a Stream only")
	events.Flags().DurationVar(&c.since, "since", 0, "When reading a Stream reads from a certain duration ago")
	flagPlaceholder(events, "since", "DURATION")
}

func init() {
	registerCommand("events", 7, configureEventsCommand)
}

func (c *eventsCmd) handleJsEvent(msg jetstream.Msg) {
	c.handleNATSEventData(msg.Subject(), msg.Data())
}

func (c *eventsCmd) handleNATSEvent(msg *nats.Msg) {
	c.handleNATSEventData(msg.Subject, msg.Data)
}

func (c *eventsCmd) handleNATSEventData(subject string, data []byte) {
	if !c.bodyFRe.MatchString(strings.ToUpper(string(data))) {
		return
	}

	if c.json && !c.ce {
		fmt.Println(string(data))
		return
	}

	handle := func() error {
		kind, event, err := api.ParseMessage(data)
		if err != nil {
			return fmt.Errorf("parsing failed: %s", err)
		}

		if opts().Trace {
			log.Printf("Received %s event on subject %s", kind, subject)
		}

		if kind == "io.nats.unknown_message" {
			return fmt.Errorf("unknown event schema on subject %s", subject)
		}

		ne, ok := event.(api.Event)
		if !ok {
			return fmt.Errorf("event %q does not implement the Event interface", kind)
		}

		var format api.RenderFormat
		switch {
		case c.ce:
			format = api.ApplicationCloudEventV1Format
		case c.short:
			format = api.TextCompactFormat
		default:
			format = api.TextExtendedFormat
		}

		err = api.RenderEvent(os.Stdout, ne, format)
		if err != nil {
			return fmt.Errorf("display failed: %s", err)
		}

		fmt.Println()

		return nil
	}

	c.Lock()
	defer c.Unlock()

	err := handle()
	if err != nil {
		fmt.Printf("Event error: %s\n\n", err)
		fmt.Println(leftPad(string(data), 10))
	}
}

func (c *eventsCmd) Printf(f string, arg ...any) {
	if !c.json {
		fmt.Printf(f, arg...)
	}
}

func (c *eventsCmd) eventsAction(_ *cobra.Command, _ []string) error {
	if c.ce {
		c.json = true
	}

	nc, _, err := prepareHelper("", natsOpts()...)
	fatalIfError(err, "setup failed")

	c.bodyFRe, err = regexp.Compile(strings.ToUpper(c.bodyF))
	fatalIfError(err, "invalid body regular expression")

	hasSubjectSelect := c.showAll || c.showJsAdvisories || c.showJsMetrics || len(c.extraSubjects) > 0
	if !hasSubjectSelect && !c.showServerAdvisories && c.stream == "" {
		return fmt.Errorf("no events were chosen")
	}
	if hasSubjectSelect && c.stream != "" {
		return fmt.Errorf("cannot specify both Stream and specific advisories or extra subjects")
	}

	if c.stream != "" {
		cfg := jetstream.OrderedConsumerConfig{}
		if c.since > 0 {
			start := time.Now().Add(-c.since)
			cfg.OptStartTime = &start
			c.Printf("Listening for Events in stream %s since %s\n", c.stream, f(start))
		} else {
			c.Printf("Listening for Events in stream %s\n", c.stream)
		}

		js, err := newJetStreamWithOptions(nc, opts())
		if err != nil {
			return err
		}

		cons, err := js.OrderedConsumer(ctx, c.stream, cfg)
		if err != nil {
			return err
		}

		cons.Consume(c.handleJsEvent)
	} else {
		if c.showJsAdvisories || c.showAll {
			c.Printf("Listening for Advisories on %s.>\n", jsm.EventSubject(api.JSAdvisoryPrefix, opts().Config.JSEventPrefix()))
			nc.Subscribe(fmt.Sprintf("%s.>", jsm.EventSubject(api.JSAdvisoryPrefix, opts().Config.JSEventPrefix())), func(m *nats.Msg) {
				c.handleNATSEvent(m)
			})
		}

		if c.showJsMetrics || c.showAll {
			c.Printf("Listening for Metrics on %s.>\n", jsm.EventSubject(api.JSMetricPrefix, opts().Config.JSEventPrefix()))
			nc.Subscribe(fmt.Sprintf("%s.>", jsm.EventSubject(api.JSMetricPrefix, opts().Config.JSEventPrefix())), func(m *nats.Msg) {
				c.handleNATSEvent(m)
			})
		}

		if c.showServerAdvisories || c.showAll {
			c.Printf("Listening for Client Connection events on $SYS.ACCOUNT.*.CONNECT\n")
			nc.Subscribe("$SYS.ACCOUNT.*.CONNECT", func(m *nats.Msg) {
				c.handleNATSEvent(m)
			})

			c.Printf("Listening for Client Disconnection events on $SYS.ACCOUNT.*.DISCONNECT\n")
			nc.Subscribe("$SYS.ACCOUNT.*.DISCONNECT", func(m *nats.Msg) {
				c.handleNATSEvent(m)
			})

			c.Printf("Listening for Authentication Errors events on $SYS.SERVER.*.CLIENT.AUTH.ERR\n")
			nc.Subscribe("$SYS.SERVER.*.CLIENT.AUTH.ERR", func(m *nats.Msg) {
				c.handleNATSEvent(m)
			})
		}

		if len(c.extraSubjects) > 0 {
			for _, s := range c.extraSubjects {
				c.Printf("Listening for advisories on %s\n", s)
				nc.Subscribe(s, func(m *nats.Msg) {
					c.handleNATSEvent(m)
				})
			}
		}
	}

	<-ctx.Done()

	return nil
}

func leftPad(s string, indent int) string {
	var out []string
	format := fmt.Sprintf("%%%ds", indent)

	for _, l := range strings.Split(s, "\n") {
		out = append(out, fmt.Sprintf(format, " ")+l)
	}

	return strings.Join(out, "\n")
}
