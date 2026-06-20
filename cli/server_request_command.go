// Copyright 2020-2026 The NATS Authors
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
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/nats-io/jsm.go/serverdata"
	"github.com/nats-io/natscli/options"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/spf13/cobra"
)

type SrvRequestCmd struct {
	name     string
	host     string
	cluster  string
	account  string
	stream   string
	consumer string
	tags     []string
	cid      uint64

	limit   int
	offset  int
	waitFor uint32

	includeAccounts   bool
	includeStreams    bool
	includeConsumers  bool
	includeConfig     bool
	leaderOnly        bool
	streamLeaderOnly  bool
	includeRaftGroups bool
	includeAll        bool
	ipqAll            bool
	includeDetails    bool

	detail               bool
	sortOpt              string
	cidFilter            uint64
	stateFilter          string
	userFilter           string
	accountFilter        string
	subjectFilter        string
	nameFilter           string
	accountSubscriptions bool

	nc *nats.Conn

	jsServerOnly bool
	jsEnabled    bool

	archivePath string

	profileName  string
	profileDebug int
	profileDir   string
	filterEmpty  bool
	group        string
	queueFilter  string
}

func configureServerRequestCommand(srv *cobra.Command) {
	c := &SrvRequestCmd{}

	req := addCommand(srv, "request", "Request monitoring data from a specific server")
	req.Aliases = []string{"req"}
	cmdAddTags(req, "scope:system", "impact:ro")
	req.PersistentFlags().IntVar(&c.limit, "limit", 2048, "Limit the responses to a certain amount of records")
	req.PersistentFlags().IntVar(&c.offset, "offset", 0, "Start at a certain record")
	req.PersistentFlags().StringVar(&c.name, "name", "", "Limit to servers matching a server name")
	req.PersistentFlags().StringVar(&c.host, "host", "", "Limit to servers matching a server host name")
	req.PersistentFlags().StringVar(&c.cluster, "cluster", "", "Limit to servers matching a cluster name")
	req.PersistentFlags().StringArrayVar(&c.tags, "tags", nil, "Limit to servers with these configured tags")

	accountz := addCommand(req, "accounts", "Show account details")
	accountz.Aliases = []string{"accountz", "acct"}
	accountz.RunE = c.accountz
	cmdAddTags(accountz, "scope:system", "impact:ro")
	addArg(accountz, "wait", "Wait for a certain number of responses", false, "uint")
	accountz.Flags().StringVar(&c.account, "account", "", "Retrieve information for a specific account")
	accountz.Flags().StringVar(&c.archivePath, "archive", "", "Read data from an archive file")

	connz := addCommand(req, "connections", "Show connection details")
	connz.Aliases = []string{"conn", "connz"}
	connz.RunE = c.conns
	cmdAddTags(connz, "scope:system", "impact:ro")
	addArg(connz, "wait", "Wait for a certain number of responses", false, "uint")
	connz.Flags().Var(newEnumValue(&c.sortOpt, "cid", "cid", "start", "subs", "pending", "msgs_to", "msgs_from", "bytes_to", "bytes_from", "last", "idle", "uptime", "stop", "reason", "rtt"), "sort", "Sort by a specific property")
	connz.Flags().BoolVar(&c.detail, "subscriptions", false, "Show subscriptions")
	connz.Flags().Uint64Var(&c.cidFilter, "filter-cid", 0, "Filter on a specific CID")
	flagPlaceholder(connz, "filter-cid", "CID")
	connz.Flags().Var(newEnumValue(&c.stateFilter, "open", "open", "closed", "all"), "filter-state", "Filter on a specific account state (open, closed, all)")
	connz.Flags().StringVar(&c.userFilter, "filter-user", "", "Filter on a specific username")
	flagPlaceholder(connz, "filter-user", "USER")
	connz.Flags().StringVar(&c.accountFilter, "filter-account", "", "Filter on a specific account")
	flagPlaceholder(connz, "filter-account", "ACCOUNT")
	connz.Flags().StringVar(&c.subjectFilter, "filter-subject", "", "Limits responses only to those connections with matching subscription interest")
	flagPlaceholder(connz, "filter-subject", "SUBJECT")
	connz.Flags().BoolVar(&c.filterEmpty, "filter-empty", false, "Only shows responses that have connections")
	connz.Flags().StringVar(&c.archivePath, "archive", "", "Read data from an archive file")

	gwyz := addCommand(req, "gateways", "Show gateway details")
	gwyz.Aliases = []string{"gateway", "gwy", "gatewayz"}
	gwyz.RunE = c.gwyz
	cmdAddTags(gwyz, "scope:system", "impact:ro")
	addArg(gwyz, "wait", "Wait for a certain number of responses", false, "uint")
	addArg(gwyz, "filter-name", "Filter results on gateway name", false, "string")
	gwyz.Flags().StringVar(&c.accountFilter, "filter-account", "", "Show only a certain account in account detail")
	flagPlaceholder(gwyz, "filter-account", "ACCOUNT")
	gwyz.Flags().BoolVar(&c.detail, "accounts", false, "Show account detail")
	negatableBoolVar(gwyz, &c.accountSubscriptions, "subscriptions", true, "Show subscription details")
	gwyz.Flags().StringVar(&c.archivePath, "archive", "", "Read data from an archive file")

	ipq := addCommand(req, "ipqueue", "Show IP Queue details")
	ipq.Aliases = []string{"ipq", "ipqueuesz"}
	ipq.RunE = c.ipqz
	cmdAddTags(ipq, "scope:system", "impact:ro")
	negatableBoolVar(ipq, &c.ipqAll, "all", true, "Shows all available information")
	ipq.Flags().StringVar(&c.queueFilter, "filter", "", "Filter results for specific queues")
	ipq.Flags().StringVar(&c.archivePath, "archive", "", "Read data from an archive file")

	healthz := addCommand(req, "jetstream-health", "Request JetStream health status")
	healthz.Aliases = []string{"healthz"}
	healthz.RunE = c.healthz
	cmdAddTags(healthz, "scope:system", "impact:ro")
	addArg(healthz, "wait", "Wait for a certain number of responses", false, "uint")
	healthz.Flags().BoolVarP(&c.jsEnabled, "js-enabled", "J", false, "Checks that JetStream should be enabled on all servers")
	healthz.Flags().BoolVarP(&c.jsServerOnly, "server-only", "S", false, "Restricts the health check to the JetStream server only, do not check streams and consumers")
	healthz.Flags().StringVar(&c.account, "account", "", "Check only a specific Account")
	healthz.Flags().StringVar(&c.stream, "stream", "", "Check only a specific Stream")
	healthz.Flags().StringVar(&c.consumer, "consumer", "", "Check only a specific Consumer")
	negatableBoolVar(healthz, &c.includeDetails, "details", true, "Include extended details about all failures")
	healthz.Flags().StringVar(&c.archivePath, "archive", "", "Read data from an archive file")

	jsz := addCommand(req, "jetstream", "Show JetStream details")
	jsz.Aliases = []string{"jsz", "js"}
	jsz.RunE = c.jsz
	cmdAddTags(jsz, "scope:system", "impact:ro")
	addArg(jsz, "wait", "Wait for a certain number of responses", false, "uint")
	jsz.Flags().StringVar(&c.account, "account", "", "Show statistics scoped to a specific account")
	jsz.Flags().BoolVar(&c.includeAccounts, "accounts", false, "Include details about accounts")
	jsz.Flags().BoolVar(&c.includeStreams, "streams", false, "Include details about Streams")
	jsz.Flags().BoolVar(&c.includeConsumers, "consumer", false, "Include details about Consumers")
	jsz.Flags().BoolVar(&c.includeConfig, "config", false, "Include details about configuration")
	jsz.Flags().BoolVar(&c.includeRaftGroups, "raft", false, "Include details about raft groups")
	jsz.Flags().BoolVar(&c.leaderOnly, "leader", false, "Request a response from the Meta-group leader only")
	jsz.Flags().BoolVar(&c.streamLeaderOnly, "stream-leader", false, "Request a response from Stream leaders only")
	jsz.Flags().BoolVar(&c.includeAll, "all", false, "Include accounts, streams, consumers and configuration")
	jsz.Flags().StringVar(&c.archivePath, "archive", "", "Read data from an archive file")

	kick := addCommand(req, "kick", "Disconnects a client immediately")
	kick.RunE = c.kick
	cmdAddTags(kick, "scope:system", "impact:rw")
	addArg(kick, "client", "The Client ID to disconnect", true, "uint")
	addArg(kick, "server", "The Server ID to disconnect the client from", true, "string")

	leafz := addCommand(req, "leafnodes", "Show leafnode details")
	leafz.Aliases = []string{"leaf", "leafz"}
	leafz.RunE = c.leafz
	cmdAddTags(leafz, "scope:system", "impact:ro")
	addArg(leafz, "wait", "Wait for a certain number of responses", false, "uint")
	leafz.Flags().BoolVar(&c.detail, "subscriptions", false, "Show subscription detail")
	leafz.Flags().StringVar(&c.archivePath, "archive", "", "Read data from an archive file")

	profilez := addCommand(req, "profile", "Run a profile")
	profilez.RunE = c.profilez
	cmdAddTags(profilez, "scope:system", "impact:ro")
	addArgEnum(profilez, "profile", "Specify the name of the profile to run (allocs, heap, goroutine, mutex, threadcreate, block, cpu)", true, "allocs", "heap", "goroutine", "mutex", "threadcreate", "block", "cpu")
	addArgWithDefault(profilez, "dir", "Set the output directory for profile files", ".", "path")
	profilez.Flags().IntVar(&c.profileDebug, "level", 0, "Set the debug level of the profile")
	profilez.Flags().StringVar(&c.archivePath, "archive", "", "Read data from an archive file")

	raftz := addCommand(req, "raft", "Show RAFT state details")
	raftz.Aliases = []string{"raftz"}
	raftz.RunE = c.raftz
	cmdAddTags(raftz, "scope:system", "impact:ro")
	raftz.Flags().StringVar(&c.account, "account", "", "Filters on an specific account")
	raftz.Flags().StringVar(&c.group, "group", "", "Filters on a specific group")
	raftz.Flags().StringVar(&c.archivePath, "archive", "", "Read data from an archive file")

	routez := addCommand(req, "routes", "Show route details")
	routez.Aliases = []string{"route", "routez"}
	routez.RunE = c.routez
	cmdAddTags(routez, "scope:system", "impact:ro")
	addArg(routez, "wait", "Wait for a certain number of responses", false, "uint")
	routez.Flags().BoolVar(&c.detail, "subscriptions", false, "Show subscription detail")
	routez.Flags().StringVar(&c.archivePath, "archive", "", "Read data from an archive file")

	subz := addCommand(req, "subscriptions", "Show subscription information")
	subz.Aliases = []string{"sub", "subsz"}
	subz.RunE = c.subs
	cmdAddTags(subz, "scope:system", "impact:ro")
	addArg(subz, "wait", "Wait for a certain number of responses", false, "uint")
	subz.Flags().BoolVar(&c.detail, "detail", false, "Include detail about all subscriptions")
	subz.Flags().StringVar(&c.accountFilter, "filter-account", "", "Filter on a specific account")
	flagPlaceholder(subz, "filter-account", "ACCOUNT")
	subz.Flags().StringVar(&c.subjectFilter, "filter-subject", "", "Filter based on subscriptions matching this subject")
	flagPlaceholder(subz, "filter-subject", "SUBJECT")
	subz.Flags().StringVar(&c.archivePath, "archive", "", "Read data from an archive file")

	varz := addCommand(req, "variables", "Show runtime variables")
	varz.Aliases = []string{"var", "varz"}
	varz.RunE = c.varz
	cmdAddTags(varz, "scope:system", "impact:ro")
	addArg(varz, "wait", "Wait for a certain number of responses", false, "uint")
	varz.Flags().StringVar(&c.archivePath, "archive", "", "Read data from an archive file")
}

func (c *SrvRequestCmd) reqFilter() server.EventFilterOptions {
	opt := server.EventFilterOptions{
		Name:       c.name,
		Host:       c.host,
		Cluster:    c.cluster,
		Tags:       c.tags,
		ExactMatch: true,
	}
	if opts().Config != nil {
		opt.Domain = opts().Config.JSDomain()
	}

	return opt
}

func (c *SrvRequestCmd) dataSource() (serverdata.Source, error) {
	if c.archivePath != "" {
		return serverdata.NewAuditArchive(c.archivePath)
	}

	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return nil, err
	}
	c.nc = nc

	waitFor := c.waitFor
	if waitFor == 0 {
		if c.host != "" || c.name != "" {
			waitFor = 1
		} else {
			w, _ := serverdata.CurrentActiveServers(ctx, nc, opts().Timeout, traceLogger())
			waitFor = uint32(w)
		}
	}

	return serverdata.NewLive(nc, func(req any, subj string, waitFor int, nc *nats.Conn) ([][]byte, error) {
		return serverdata.DoReq(ctx, req, subj, waitFor, nc, opts().Timeout, traceLogger())
	}, int(waitFor))
}

// bindWaitArg binds the optional "wait" positional argument at index idx to c.waitFor.
func (c *SrvRequestCmd) bindWaitArg(args []string, idx int) error {
	if v := argValue(args, idx); v != "" {
		w, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return err
		}
		c.waitFor = uint32(w)
	}
	return nil
}

func printResults[T any](results []*T) error {
	for _, r := range results {
		j, err := json.Marshal(r)
		if err != nil {
			return err
		}
		fmt.Println(string(j))
	}
	return nil
}

func (c *SrvRequestCmd) ipqz(_ *cobra.Command, _ []string) error {
	src, err := c.dataSource()
	if err != nil {
		return err
	}
	defer src.Close()

	responses, err := src.Ipqueuesz(server.IpqueueszEventOptions{
		IpqueueszOptions: server.IpqueueszOptions{
			All:    c.ipqAll,
			Filter: c.queueFilter,
		},
		EventFilterOptions: c.reqFilter(),
	})
	if err != nil {
		return err
	}

	return printResults(responses)
}

func (c *SrvRequestCmd) raftz(_ *cobra.Command, _ []string) error {
	src, err := c.dataSource()
	if err != nil {
		return err
	}
	defer src.Close()

	responses, err := src.Raftz(server.RaftzEventOptions{
		RaftzOptions: server.RaftzOptions{
			AccountFilter: c.account,
			GroupFilter:   c.group,
		},
		EventFilterOptions: c.reqFilter(),
	})
	if err != nil {
		return err
	}

	return printResults(responses)
}

func (c *SrvRequestCmd) kick(_ *cobra.Command, args []string) error {
	cid, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return err
	}
	c.cid = cid
	c.host = args[1]

	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	res, err := serverdata.DoReq(ctx, &server.KickClientReq{CID: c.cid}, fmt.Sprintf("$SYS.REQ.SERVER.%s.KICK", c.host), 1, nc, opts().Timeout, traceLogger())
	if err != nil {
		return err
	}

	if len(res) == 0 {
		return fmt.Errorf("no responses received")
	}

	for _, m := range res {
		var b bytes.Buffer
		err := json.Indent(&b, m, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(b.String())
	}

	return nil
}

func (c *SrvRequestCmd) profilez(_ *cobra.Command, args []string) error {
	c.profileName = args[0]
	c.profileDir = "."
	if v := argValue(args, 1); v != "" {
		c.profileDir = v
	}

	reqOpts := server.ProfilezEventOptions{
		ProfilezOptions: server.ProfilezOptions{
			Name:  c.profileName,
			Debug: c.profileDebug,
		},
		EventFilterOptions: c.reqFilter(),
	}

	if c.archivePath == "" && c.profileName == "cpu" {
		// people can use --timeout to adjust the wait time
		reqOpts.Duration = options.DefaultOptions.Timeout
		// but we have to then bump timeout to give the network time
		options.DefaultOptions.Timeout = options.DefaultOptions.Timeout + 2*time.Second
	}

	src, err := c.dataSource()
	if err != nil {
		return err
	}
	defer src.Close()

	responses, err := src.Profilez(reqOpts)
	if err != nil {
		return err
	}

	if len(responses) == 0 && c.archivePath != "" {
		fmt.Fprintf(os.Stderr, "no captured profiles matched %q at level %d in %s; audit gather typically captures goroutine at level 1 and 2\n", c.profileName, c.profileDebug, c.archivePath)
	}

	prefix := fmt.Sprintf("%s-%s-", c.profileName, time.Now().Format("20060102-150405"))
	prefix = filepath.Join(c.profileDir, prefix)

	for _, resp := range responses {
		if resp.Data != nil && resp.Data.Error != "" {
			fmt.Fprintf(os.Stderr, "Server %q error: %s\n", resp.Server.Name, resp.Data.Error)
			continue
		}
		if resp.Data == nil {
			continue
		}

		filename := prefix + resp.Server.Name
		if err := c.profilezWrite(filename, resp); err != nil {
			fmt.Fprintf(os.Stderr, "Server %q error: %s\n", resp.Server.Name, err)
		} else {
			fmt.Fprintf(os.Stdout, "Server %q profile written: %s\n", resp.Server.Name, filename)
		}
	}

	return nil
}

func (c *SrvRequestCmd) profilezWrite(filename string, resp *serverdata.ProfilezResponse) error {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	n, err := f.Write(resp.Data.Profile)
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}
	if n != len(resp.Data.Profile) {
		return fmt.Errorf("short write")
	}

	return nil
}

func (c *SrvRequestCmd) healthz(_ *cobra.Command, args []string) error {
	if err := c.bindWaitArg(args, 0); err != nil {
		return err
	}

	src, err := c.dataSource()
	if err != nil {
		return err
	}
	defer src.Close()

	responses, err := src.Healthz(server.HealthzEventOptions{
		HealthzOptions: server.HealthzOptions{
			JSEnabledOnly: c.jsEnabled,
			JSServerOnly:  c.jsServerOnly,
			Details:       c.includeDetails,
			Account:       c.account,
			Stream:        c.stream,
			Consumer:      c.consumer,
		},
		EventFilterOptions: c.reqFilter(),
	})
	if err != nil {
		return err
	}

	return printResults(responses)
}

func (c *SrvRequestCmd) jsz(_ *cobra.Command, args []string) error {
	if err := c.bindWaitArg(args, 0); err != nil {
		return err
	}

	// we expect response only from the meta leader node
	if c.leaderOnly {
		c.waitFor = 1
	}

	src, err := c.dataSource()
	if err != nil {
		return err
	}
	defer src.Close()

	opts := server.JszEventOptions{
		JSzOptions: server.JSzOptions{
			Account:          c.account,
			LeaderOnly:       c.leaderOnly,
			StreamLeaderOnly: c.streamLeaderOnly,
			Offset:           c.offset,
			Limit:            c.limit,
		},
		EventFilterOptions: c.reqFilter(),
	}

	if c.includeAccounts || c.includeAll {
		opts.JSzOptions.Accounts = true
	}
	if c.includeStreams || c.includeAll {
		opts.JSzOptions.Streams = true
	}
	if c.includeConsumers || c.includeAll {
		opts.JSzOptions.Consumer = true
	}
	if c.includeConfig || c.includeAll {
		opts.JSzOptions.Config = true
	}
	if c.includeRaftGroups || c.includeAll {
		opts.JSzOptions.RaftGroups = true
	}

	responses, err := src.Jsz(opts)
	if err != nil {
		return err
	}

	return printResults(responses)
}

func (c *SrvRequestCmd) accountz(_ *cobra.Command, args []string) error {
	if err := c.bindWaitArg(args, 0); err != nil {
		return err
	}

	src, err := c.dataSource()
	if err != nil {
		return err
	}
	defer src.Close()

	responses, err := src.Accountz(server.AccountzEventOptions{
		AccountzOptions:    server.AccountzOptions{Account: c.account},
		EventFilterOptions: c.reqFilter(),
	})
	if err != nil {
		return err
	}

	return printResults(responses)
}

func (c *SrvRequestCmd) leafz(_ *cobra.Command, args []string) error {
	if err := c.bindWaitArg(args, 0); err != nil {
		return err
	}

	src, err := c.dataSource()
	if err != nil {
		return err
	}
	defer src.Close()

	responses, err := src.Leafz(server.LeafzEventOptions{
		LeafzOptions:       server.LeafzOptions{Subscriptions: c.detail},
		EventFilterOptions: c.reqFilter(),
	})
	if err != nil {
		return err
	}

	return printResults(responses)
}

func (c *SrvRequestCmd) gwyz(_ *cobra.Command, args []string) error {
	if err := c.bindWaitArg(args, 0); err != nil {
		return err
	}
	c.nameFilter = argValue(args, 1)

	if c.accountFilter != "" {
		c.detail = true
	}

	src, err := c.dataSource()
	if err != nil {
		return err
	}
	defer src.Close()

	opts := server.GatewayzEventOptions{
		GatewayzOptions: server.GatewayzOptions{
			Name:        c.nameFilter,
			Accounts:    c.detail,
			AccountName: c.accountFilter,
		},
		EventFilterOptions: c.reqFilter(),
	}

	if c.accountFilter != "" && c.detail {
		opts.GatewayzOptions.AccountSubscriptions = c.accountSubscriptions
		opts.GatewayzOptions.AccountSubscriptionsDetail = c.accountSubscriptions
	}

	responses, err := src.Gatewayz(opts)
	if err != nil {
		return err
	}

	return printResults(responses)
}

func (c *SrvRequestCmd) routez(_ *cobra.Command, args []string) error {
	if err := c.bindWaitArg(args, 0); err != nil {
		return err
	}

	src, err := c.dataSource()
	if err != nil {
		return err
	}
	defer src.Close()

	responses, err := src.Routez(server.RoutezEventOptions{
		RoutezOptions: server.RoutezOptions{
			Subscriptions:       c.detail,
			SubscriptionsDetail: c.detail,
		},
		EventFilterOptions: c.reqFilter(),
	})
	if err != nil {
		return err
	}

	return printResults(responses)
}

func (c *SrvRequestCmd) conns(_ *cobra.Command, args []string) error {
	if err := c.bindWaitArg(args, 0); err != nil {
		return err
	}

	src, err := c.dataSource()
	if err != nil {
		return err
	}
	defer src.Close()

	opts := server.ConnzEventOptions{
		ConnzOptions: server.ConnzOptions{
			Sort:                server.SortOpt(c.sortOpt),
			Username:            true,
			Subscriptions:       c.detail,
			SubscriptionsDetail: c.detail,
			Offset:              c.offset,
			Limit:               c.limit,
			CID:                 c.cidFilter,
			User:                c.userFilter,
			Account:             c.accountFilter,
			FilterSubject:       c.subjectFilter,
		},
		EventFilterOptions: c.reqFilter(),
	}

	switch c.stateFilter {
	case "all":
		opts.State = server.ConnAll
	case "closed":
		opts.State = server.ConnClosed
	default:
		opts.State = server.ConnOpen
	}

	responses, err := src.Connz(opts)
	if err != nil {
		return err
	}

	for _, r := range responses {
		if c.filterEmpty && (r.Data == nil || r.Data.NumConns == 0) {
			continue
		}

		j, err := json.Marshal(r)
		if err != nil {
			return err
		}
		fmt.Println(string(j))
	}

	return nil
}

func (c *SrvRequestCmd) varz(_ *cobra.Command, args []string) error {
	if err := c.bindWaitArg(args, 0); err != nil {
		return err
	}

	src, err := c.dataSource()
	if err != nil {
		return err
	}
	defer src.Close()

	responses, err := src.Varz(server.VarzEventOptions{
		EventFilterOptions: c.reqFilter(),
	})
	if err != nil {
		return err
	}

	return printResults(responses)
}

func (c *SrvRequestCmd) subs(_ *cobra.Command, args []string) error {
	if err := c.bindWaitArg(args, 0); err != nil {
		return err
	}

	src, err := c.dataSource()
	if err != nil {
		return err
	}
	defer src.Close()

	responses, err := src.Subsz(server.SubszEventOptions{
		SubszOptions: server.SubszOptions{
			Offset:        c.offset,
			Limit:         c.limit,
			Subscriptions: c.detail,
			Account:       c.accountFilter,
			Test:          c.subjectFilter,
		},
		EventFilterOptions: c.reqFilter(),
	})
	if err != nil {
		return err
	}

	return printResults(responses)
}
