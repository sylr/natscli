// Copyright 2024-2025 The NATS Authors
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

	"github.com/nats-io/jsm.go/api"
	gatherer "github.com/nats-io/jsm.go/audit/gather"

	"github.com/spf13/cobra"
)

type auditGatherCmd struct {
	progress bool
	config   *gatherer.Configuration
}

func configureAuditGatherCommand(app *cobra.Command) {
	c := &auditGatherCmd{
		config: gatherer.NewCaptureConfiguration(),
	}

	gather := addCommand(app, "gather", "capture a variety of data from a deployment into an archive file")
	gather.Aliases = []string{"capture", "cap"}
	gather.RunE = c.gather
	cmdAddTags(gather, "scope:system", "impact:ro")
	gather.Flags().StringVarP(&c.config.TargetPath, "output", "o", "", "output file path of generated archive")
	negatableBoolVar(gather, &c.progress, "progress", true, "Display progress messages during gathering")
	gather.Flags().StringVar(&c.config.ClusterFilter, "cluster", "", "Limit audit to servers from the named cluster")
	flagPlaceholder(gather, "cluster", "CLUSTER")
	negatableBoolVar(gather, &c.config.Include.ServerEndpoints, "server-endpoints", true, "Capture monitoring endpoints for each server")
	negatableBoolVar(gather, &c.config.Include.ServerProfiles, "server-profiles", true, "Capture profiles for each server")
	negatableBoolVar(gather, &c.config.Include.AccountEndpoints, "account-endpoints", true, "Capture monitoring endpoints for each account")
	negatableBoolVar(gather, &c.config.Include.Streams, "streams", true, "Capture state of each stream")
	negatableBoolVar(gather, &c.config.Include.Consumers, "consumers", true, "Capture state of each stream consumers")
	negatableBoolVar(gather, &c.config.Detailed, "details", true, "Capture detailed server information from the audit")
}

func (c *auditGatherCmd) gather(_ *cobra.Command, _ []string) error {
	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	c.config.Version = fmt.Sprintf("nats %s", Version)

	switch {
	case opts().Trace:
		c.config.LogLevel = api.TraceLevel
	case c.progress:
		c.config.LogLevel = api.InfoLevel
	default:
		c.config.LogLevel = api.ErrorLevel
	}

	c.config.Timeout = opts().Timeout

	return gatherer.Gather(nc, c.config)
}
