// Copyright 2019-2025 The NATS Authors
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
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/jsm.go/serverdata"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/natscli/columns"
	iu "github.com/nats-io/natscli/internal/util"
	"github.com/spf13/cobra"
)

type actCmd struct {
	sort    string
	subject string
	topk    int

	backupDirectory   string
	healthCheck       bool
	snapShotConsumers bool
	force             bool
	failOnWarn        bool

	placementCluster string
	placementTags    []string
	reverse          bool
	stateFilter      string
	user             string
	filterReason     string
}

func configureActCommand(app commandHost) {
	c := &actCmd{}
	act := addCommand(app, "account", "Account information and status")
	act.Aliases = []string{"a"}
	addCheat("account", act)

	info := addCommand(act, "info", "Account information")
	info.RunE = c.infoAction
	cmdAddTags(info, "scope:user", "impact:ro")

	report := addCommand(act, "report", "Report on account metrics")
	report.Aliases = []string{"rep"}
	cmdAddTags(report, "scope:user", "impact:ro")

	conns := addCommand(report, "connections", "Report on connections")
	conns.Aliases = []string{"conn", "connz", "conns"}
	conns.RunE = c.reportConnectionsAction
	cmdAddTags(conns, "scope:user", "impact:ro")
	conns.Flags().Var(newEnumValue(&c.sort, "subs", "in-bytes", "out-bytes", "in-msgs", "out-msgs", "uptime", "cid", "subs"), "sort", "Sort by a specific property (in-bytes,out-bytes,in-msgs,out-msgs,uptime,cid,subs)")
	conns.Flags().IntVar(&c.topk, "top", 1000, "Limit results to the top results")
	conns.Flags().StringVar(&c.subject, "subject", "", "Limits responses only to those connections with matching subscription interest")
	conns.Flags().StringVar(&c.user, "username", "", "Limits responses only to those connections for a specific authentication username")
	conns.Flags().BoolVarP(&c.reverse, "reverse", "R", false, "Reverse sort connections")
	conns.Flags().Var(newEnumValue(&c.stateFilter, "open", "open", "closed", "all"), "state", "Limits responses only to those connections that are in a specific state (open, closed, all)")
	conns.Flags().StringVar(&c.filterReason, "closed-reason", "", "Filter results based on a closed reason")
	flagPlaceholder(conns, "closed-reason", "REASON")

	stats := addCommand(report, "statistics", "Report on server statistics")
	stats.Aliases = []string{"stats", "statsz"}
	stats.RunE = c.reportServerStats
	cmdAddTags(stats, "scope:user", "impact:ro")

	backup := addCommand(act, "backup", "Creates a backup of all  JetStream Streams over the NATS network")
	backup.Aliases = []string{"snapshot"}
	backup.RunE = c.backupAction
	cmdAddTags(backup, "scope:user", "impact:ro")
	addArg(backup, "target", "Directory to create the backup in", true, "string")
	backup.Flags().BoolVar(&c.healthCheck, "check", false, "Checks the Stream for health prior to backup")
	negatableBoolVar(backup, &c.snapShotConsumers, "consumers", true, "Enable or disable consumer backups")
	backup.Flags().BoolVarP(&c.force, "force", "f", false, "Perform backup without prompting")
	backup.Flags().BoolVarP(&c.failOnWarn, "critical-warnings", "w", false, "Treat warnings as failures")

	restore := addCommand(act, "restore", "Restore an account backup over the NATS network")
	restore.RunE = c.restoreAction
	cmdAddTags(restore, "scope:user", "impact:rw")
	addArg(restore, "directory", "The directory holding the account backup to restore", true, "string")
	restore.Flags().StringVar(&c.placementCluster, "cluster", "", "Place the stream in a specific cluster")
	restore.Flags().StringArrayVar(&c.placementTags, "tag", nil, "Place the stream on servers that has specific tags (pass multiple times)")

	configureAccountTLSCommand(act)
}

func init() {
	registerCommand("account", 0, configureActCommand)
}

func (c *actCmd) backupAction(_ *cobra.Command, args []string) error {
	c.backupDirectory = args[0]

	var err error

	_, mgr, err := prepareHelper("", natsOpts()...)
	fatalIfError(err, "setup failed")

	streams, missing, offline, err := mgr.Streams(nil)
	if err != nil {
		return err
	}

	if len(missing) > 0 {
		return fmt.Errorf("could not obtain stream information for %d streams", len(missing))
	}
	if !c.force && len(offline) > 0 {
		return fmt.Errorf("could not obtain stream information for %d offline streams", len(offline))
	}
	if len(streams) == 0 {
		return fmt.Errorf("no streams found")
	}

	totalSize := uint64(0)
	totalConsumers := 0

	for _, s := range streams {
		state, _ := s.LatestState()
		totalConsumers += state.Consumers
		totalSize += state.Bytes
	}

	cols := newColumnsf("Performing backup of all streams to %s", c.backupDirectory)
	cols.AddRow("Streams", len(streams))
	cols.AddRow("Size", humanize.IBytes(totalSize))
	cols.AddRow("Consumers:", totalConsumers)
	cols.Println()
	cols.Frender(os.Stdout)

	if !c.force {
		ok, err := askConfirmation("Perform backup", false)
		if err != nil {
			return err
		}

		if !ok {
			return nil
		}
	}

	err = os.MkdirAll(c.backupDirectory, 0700)
	if err != nil {
		return err
	}

	var errs []error
	var warns []error

	for _, s := range streams {
		err = backupStream(s, false, c.snapShotConsumers, c.healthCheck, filepath.Join(c.backupDirectory, s.Name()), 128*1024, 0)
		if errors.Is(err, jsm.ErrMemoryStreamNotSupported) {
			fmt.Printf("Backup of %s failed: %v\n", s.Name(), err)
			warns = append(warns, fmt.Errorf("%s: %w", s.Name(), err))
		} else if err != nil {
			fmt.Printf("Backup of %s failed: %s\n", s.Name(), err)
			errs = append(errs, fmt.Errorf("%s: %s", s.Name(), err))
		}
		fmt.Println()
	}

	if len(warns) > 0 {
		fmt.Printf("Backup Warnings: \n")
		for _, err := range warns {
			fmt.Printf("  %s\n", err)
		}
		fmt.Println()
	}

	if len(errs) > 0 {
		fmt.Printf("Backup failures: \n")
		for _, err := range errs {
			fmt.Printf("  %s\n", err)
		}
		fmt.Println()
	}

	if len(errs) > 0 || len(warns) > 0 && c.failOnWarn {
		return fmt.Errorf("backup failed")
	}

	return nil
}

func (c *actCmd) restoreAction(_ *cobra.Command, args []string) error {
	c.backupDirectory = args[0]

	_, mgr, err := prepareHelper("", natsOpts()...)
	fatalIfError(err, "setup failed")
	streams, err := mgr.StreamNames(nil)
	if err != nil {
		return err
	}
	existingStreams := map[string]struct{}{}
	for _, n := range streams {
		existingStreams[n] = struct{}{}
	}
	de, err := os.ReadDir(c.backupDirectory)
	fatalIfError(err, "setup failed")
	for _, d := range de {
		if !d.IsDir() {
			fatalf("expected a directory %q", d.Name())
		}
		if _, ok := existingStreams[d.Name()]; ok {
			fatalf("stream %q exists already", d.Name())
		}
		_, err := os.Stat(filepath.Join(c.backupDirectory, d.Name(), "backup.json"))
		fatalIfError(err, "expected backup.json")
	}
	fmt.Printf("Restoring backup of all %d streams in directory %q\n\n", len(de), c.backupDirectory)
	s := &streamCmd{msgID: -1, showProgress: false, placementCluster: c.placementCluster, placementTags: c.placementTags}
	for _, d := range de {
		s.backupDirectory = filepath.Join(c.backupDirectory, d.Name())
		err := s.restoreAction(nil, nil)
		fatalIfError(err, "restore for %s failed", d.Name())
	}
	return nil
}

func (c *actCmd) reportConnectionsAction(_ *cobra.Command, _ []string) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	cmd := SrvReportCmd{
		topk:                    c.topk,
		sort:                    c.sort,
		subject:                 c.subject,
		reverse:                 c.reverse,
		stateFilter:             c.stateFilter,
		filterReason:            c.filterReason,
		user:                    c.user,
		skipDiscoverClusterSize: true,
		nc:                      nc,
	}

	return cmd.reportConnections(nil, nil)
}

func (c *actCmd) reportServerStats(_ *cobra.Command, _ []string) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	res, err := serverdata.DoReq(ctx, nil, "$SYS.REQ.ACCOUNT.PING.STATZ", 0, nc, opts().Timeout, traceLogger())
	if err != nil {
		return err
	}

	if len(res) == 0 {
		return fmt.Errorf("did not get results from any servers")
	}

	table := iu.NewTableWriterf(opts(), "Server Statistics")
	table.AddHeaders("Server", "Cluster", "Version", "Tags", "Connections", "Subscriptions", "Leafnodes", "Sent Bytes", "Sent Messages", "Received Bytes", "Received Messages", "Slow Consumers")

	var (
		conn, ln           int
		sb, sm, rb, rm, sc int64
	)
	for _, r := range res {
		sz, err := c.parseAccountStatResp(r)
		if err != nil {
			return err
		}

		if len(sz.Stats.Accounts) == 0 {
			continue
		}

		stats := sz.Stats.Accounts[0]

		conn += stats.Conns
		ln += stats.LeafNodes
		sb += stats.Sent.Bytes
		sm += stats.Sent.Msgs
		rb += stats.Received.Bytes
		rm += stats.Received.Msgs
		sc += stats.SlowConsumers

		table.AddRow(
			sz.ServerInfo.Host,
			sz.ServerInfo.Cluster,
			sz.ServerInfo.Version,
			f(sz.ServerInfo.Tags),
			f(stats.Conns),
			f(stats.NumSubs),
			f(stats.LeafNodes),
			humanize.IBytes(uint64(stats.Sent.Bytes)),
			f(stats.Sent.Msgs),
			humanize.IBytes(uint64(stats.Received.Bytes)),
			f(stats.Received.Msgs),
			f(stats.SlowConsumers),
		)
	}
	table.AddFooter(len(res), "", "", "", f(conn), f(ln), humanize.IBytes(uint64(sb)), f(sm), humanize.IBytes(uint64(rb)), f(rm), f(sc))
	fmt.Print(table.Render())
	fmt.Println()

	return nil
}

func (c *actCmd) parseAccountStatResp(resp []byte) (*accountStats, error) {
	reqresp := map[string]json.RawMessage{}
	err := json.Unmarshal(resp, &reqresp)
	if err != nil {
		return nil, err
	}

	errresp, ok := reqresp["error"]
	if ok {
		res := map[string]any{}
		err := json.Unmarshal(errresp, &res)
		if err != nil {
			return nil, fmt.Errorf("invalid response received: %q", errresp)
		}

		msg, ok := res["description"]
		if !ok {
			return nil, fmt.Errorf("invalid response received: %q", errresp)
		}

		return nil, fmt.Errorf("invalid response received: %v", msg)
	}

	data, ok := reqresp["data"]
	if !ok {
		return nil, fmt.Errorf("no data received in response: %#v", reqresp)
	}

	sz := &accountStats{Stats: &server.AccountStatz{}, ServerInfo: &server.ServerInfo{}}
	s, ok := reqresp["server"]
	if !ok {
		return nil, fmt.Errorf("no server data received in response: %#v", reqresp)
	}
	err = json.Unmarshal(s, sz.ServerInfo)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(data, sz.Stats)
	if err != nil {
		return nil, err
	}

	return sz, nil
}

type accountStats struct {
	Stats      *server.AccountStatz
	ServerInfo *server.ServerInfo
}

func (c *actCmd) renderTier(cols *columns.Writer, name string, tier api.JetStreamTier) {
	cols.Indent(2)
	defer cols.Indent(0)

	cols.AddSectionTitle("Tier: %s", name)
	cols.Indent(6)
	cols.AddSectionTitle("Configuration Requirements")
	cols.AddRow("Stream Requires Max Bytes Set", tier.Limits.MaxBytesRequired)
	if tier.Limits.MaxAckPending <= 0 {
		cols.AddRow("Consumer Maximum Ack Pending", "Unlimited")
	} else {
		cols.AddRow("Consumer Maximum Ack Pending", tier.Limits.MaxAckPending)
	}
	cols.AddSectionTitle("Stream Resource Usage Limits")

	reservedMem := ""
	if tier.ReservedMemory > 0 {
		reservedMem = fmt.Sprintf("(%s reserved)", humanize.IBytes(tier.ReservedMemory))
	}
	if tier.Limits.MaxMemory == -1 {
		cols.AddRowf("Memory", "%s of Unlimited %s", humanize.IBytes(tier.Memory), reservedMem)
	} else {
		cols.AddRowf("Memory", "%s of %s %s", humanize.IBytes(tier.Memory), humanize.IBytes(uint64(tier.Limits.MaxMemory)), reservedMem)
	}

	if tier.Limits.MemoryMaxStreamBytes <= 0 {
		cols.AddRow("Memory Per Stream", "Unlimited")
	} else {
		cols.AddRow("Memory Per Stream", humanize.IBytes(uint64(tier.Limits.MemoryMaxStreamBytes)))
	}

	reservedStore := ""
	if tier.ReservedStore > 0 {
		reservedStore = fmt.Sprintf("(%s reserved)", humanize.IBytes(tier.ReservedStore))
	}

	if tier.Limits.MaxStore == -1 {
		cols.AddRowf("Storage", "%s of Unlimited %s", humanize.IBytes(tier.Store), reservedStore)
	} else {
		cols.AddRowf("Storage", "%s of %s %s", humanize.IBytes(tier.Store), humanize.IBytes(uint64(tier.Limits.MaxStore)), reservedStore)
	}

	if tier.Limits.StoreMaxStreamBytes <= 0 {
		cols.AddRow("Storage Per Stream", "Unlimited")
	} else {
		cols.AddRow("Storage Per Stream", humanize.IBytes(uint64(tier.Limits.StoreMaxStreamBytes)))
	}

	if tier.Limits.MaxStreams == -1 {
		cols.AddRowf("Streams", "%s of Unlimited", f(tier.Streams))
	} else {
		cols.AddRowf("Streams", "%s of %s", f(tier.Streams), f(tier.Limits.MaxStreams))
	}

	if tier.Limits.MaxConsumers == -1 {
		cols.AddRowf("Consumers", "Unlimited")
	} else {
		cols.AddRowf("Consumers", "Maximum %s per stream", f(tier.Limits.MaxConsumers))
	}
}

func (c *actCmd) infoAction(_ *cobra.Command, _ []string) error {
	nc, mgr, err := prepareHelper("", natsOpts()...)
	fatalIfError(err, "setup failed")

	id, _ := nc.GetClientID()
	ip, _ := nc.GetClientIP()
	lip := nc.LocalAddr()
	rtt, _ := nc.RTT()
	tlsc, _ := nc.TLSConnectionState()

	var ui *server.UserInfo
	if iu.ServerMinVersion(nc, 2, 10, 0) {
		subj := "$SYS.REQ.USER.INFO"
		if opts().Trace {
			log.Printf(">>> %s: {}\n", subj)
		}
		resp, err := nc.Request("$SYS.REQ.USER.INFO", nil, time.Second)
		if err == nil {
			if opts().Trace {
				log.Printf("<<< %s", string(resp.Data))
			}
			var res = struct {
				Data   *server.UserInfo  `json:"data"`
				Server server.ServerInfo `json:"server"`
				Error  *server.ApiError  `json:"error"`
			}{}

			err = json.Unmarshal(resp.Data, &res)
			if err == nil && res.Error == nil {
				ui = res.Data
			}
		}
	}

	cols := newColumnsf("Account Information")
	defer cols.Frender(os.Stdout)

	if ui != nil {
		if ui.UserName != "" {
			cols.AddRowf("User", "%s (%s)", ui.UserName, ui.UserID)
		} else {
			cols.AddRowf("User", "%s", ui.UserID)
		}
		if ui.AccountName != "" {
			cols.AddRowf("Account", "%s (%s)", ui.AccountName, ui.Account)
			cols.AddRow("System Account", nc.IsSystemAccount())
		} else {
			cols.AddRow("Account", ui.Account)
		}
		if ui.Expires == 0 {
			cols.AddRow("Expires", "never")
		} else {
			cols.AddRow("Expires", ui.Expires)
		}
	}
	cols.AddRowf("Client ID", "%d", id)
	if lip == "" || strings.HasPrefix(lip, ip.String()) {
		cols.AddRow("Client IP", ip)
	} else {
		cols.AddRowf("Client IP", "%s (%s)", ip, lip)
	}
	cols.AddRow("RTT", rtt)
	cols.AddRow("Headers Supported", nc.HeadersSupported())
	cols.AddRow("Maximum Payload", humanize.IBytes(uint64(nc.MaxPayload())))
	cols.AddRowIfNotEmpty("Connected Cluster", nc.ConnectedClusterName())
	cols.AddRow("Connected URL", nc.ConnectedUrl())
	cols.AddRow("Connected Address", nc.ConnectedAddr())
	cols.AddRow("Connected Server ID", nc.ConnectedServerId())
	cols.AddRow("Connected Server Version", nc.ConnectedServerVersion())
	cols.AddRowIf("Connected Server Name", nc.ConnectedServerName(), nc.ConnectedServerId() != nc.ConnectedServerName())

	if tlsc.HandshakeComplete {
		version := ""
		switch tlsc.Version {
		case tls.VersionTLS10:
			version = "1.0"
		case tls.VersionTLS11:
			version = "1.1"
		case tls.VersionTLS12:
			version = "1.2"
		case tls.VersionTLS13:
			version = "1.3"
		default:
			version = fmt.Sprintf("unknown (%x)", tlsc.Version)
		}

		cols.AddRowf("TLS Version", "%s using %s", version, tls.CipherSuiteName(tlsc.CipherSuite))
		cols.AddRow("TLS Server Name", tlsc.ServerName)
		if len(tlsc.VerifiedChains) > 0 {
			cols.AddRowf("TLS Verified", "issuer %s", tlsc.PeerCertificates[0].Issuer.String())
		} else {
			cols.AddRow("TLS Verified", "no")
		}
	} else {
		cols.AddRow("TLS Connection", "no")
	}

	renderPerm := func(title string, p *server.SubjectPermission) {
		if p == nil {
			return
		}
		if len(p.Deny) == 0 && len(p.Allow) == 0 {
			return
		}

		cols.Indent(2)
		defer cols.Indent(0)

		cols.Println(title)

		if len(p.Allow) > 0 {
			sort.Strings(p.Allow)
			cols.AddStringsAsValue("Allow", p.Allow)
		}

		if len(p.Deny) > 0 {
			cols.Println()
			sort.Strings(p.Deny)
			cols.AddStringsAsValue("Deny", p.Deny)
		}
	}

	if ui != nil && ui.Permissions != nil {
		cols.AddSectionTitle("Connection Permissions")
		renderPerm("Publish:", ui.Permissions.Publish)
		cols.Println()
		renderPerm("Subscribe:", ui.Permissions.Subscribe)
		cols.Println()
	}

	info, err := mgr.JetStreamAccountInfo()
	if info != nil {
		if info.Domain == "" {
			cols.AddSectionTitle("JetStream Account Information")
		} else {
			cols.AddSectionTitle("JetStream Account Information for domain %s", info.Domain)
		}
	}

	switch err {
	case nil:
		cols.AddSectionTitle("Account Usage")
		cols.AddRowIfNotEmpty("Domain", info.Domain)
		cols.AddRow("Storage", humanize.IBytes(info.Store))
		cols.AddRow("Memory", humanize.IBytes(info.Memory))
		cols.AddRow("Streams", info.Streams)
		cols.AddRow("Consumers", info.Consumers)

		cols.AddSectionTitle("Account Limits")
		cols.AddRow("Max Message Payload", humanize.IBytes(uint64(nc.MaxPayload())))

		if len(info.Tiers) > 0 {
			var tiers []string
			for n := range info.Tiers {
				tiers = append(tiers, n)
			}
			sort.Strings(tiers)

			for _, n := range tiers {
				c.renderTier(cols, n, info.Tiers[n])
			}
		} else {
			c.renderTier(cols, "Default", info.JetStreamTier)
		}

	case context.DeadlineExceeded:
		cols.Println()
		cols.Println("   No response from JetStream server")
	case nats.ErrNoResponders, nats.ErrJetStreamNotEnabled:
		cols.Println()
		cols.Println("   JetStream is not supported in this account")
	default:
		cols.Println()
		cols.Println("   Could not obtain account information: ", err.Error())
	}

	return nil
}
