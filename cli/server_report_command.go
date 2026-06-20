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
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"syscall"
	"time"

	"github.com/nats-io/jsm.go/serverdata"
	iu "github.com/nats-io/natscli/internal/util"

	"github.com/dustin/go-humanize"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/fatih/color"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

type SrvReportCmd struct {
	json bool

	filterExpression        string
	account                 string
	user                    string
	waitFor                 int
	sort                    string
	topk                    int
	reverse                 bool
	compact                 bool
	reportLeaderDistrib     bool
	subject                 string
	server                  string
	cluster                 string
	tags                    []string
	stateFilter             string
	filterReason            string
	skipDiscoverClusterSize bool
	archivePath             string
	gatewayName             string
	jsEnabled               bool
	jsServerOnly            bool
	stream                  string
	consumer                string
	watchInterval           int
	nc                      *nats.Conn
	apiLevel                uint
	all                     bool
}

type srvReportAccountInfo struct {
	Account     string               `json:"account"`
	Connections int                  `json:"connections"`
	ConnInfo    []connInfo           `json:"connection_info"`
	InMsgs      int64                `json:"in_msgs"`
	OutMsgs     int64                `json:"out_msgs"`
	InBytes     int64                `json:"in_bytes"`
	OutBytes    int64                `json:"out_bytes"`
	Subs        int                  `json:"subscriptions"`
	Server      []*server.ServerInfo `json:"server"`
}

func configureServerReportCommand(srv *cobra.Command) {
	c := &SrvReportCmd{}

	report := addCommand(srv, "report", "Report on various server metrics")
	report.Aliases = []string{"rep"}
	report.Flags().BoolVarP(&c.reverse, "reverse", "R", false, "Reverse sort connections")

	addFilterOpts := func(cmd *cobra.Command) {
		cmd.Flags().StringVar(&c.server, "host", "", "Limit the report to a specific NATS server")
		cmd.Flags().StringVar(&c.cluster, "cluster", "", "Limit the report to a specific Cluster")
		cmd.Flags().StringArrayVar(&c.tags, "tags", nil, "Limit the report to nodes matching certain tags")
		cmd.Flags().IntVar(&c.watchInterval, "watch", 0, "Display the results and update it every (WATCH) seconds")
	}

	acct := addCommand(report, "accounts", "Report on account activity")
	acct.Aliases = []string{"acct"}
	acct.RunE = c.withWatcher(c.reportAccount)
	cmdAddTags(acct, "scope:system", "impact:ro")
	addArg(acct, "account", "Account to produce a report for", false, "string")
	addArg(acct, "limit", "Limit the responses to a certain amount of servers", false, "int")
	acct.Flags().Var(newEnumValue(&c.sort, "subs", "in-bytes", "out-bytes", "in-msgs", "out-msgs", "conns", "subs"), "sort", "Sort by a specific property (in-bytes,out-bytes,in-msgs,out-msgs,conns,subs)")
	acct.Flags().IntVar(&c.topk, "top", 1000, "Limit results to the top results")
	addFilterOpts(acct)
	acct.Flags().StringVar(&c.archivePath, "archive", "", "Read data from an archive file")
	acct.Flags().BoolVarP(&c.json, "json", "j", false, "Produce JSON output")

	conns := addCommand(report, "connections", "Report on connections")
	conns.Aliases = []string{"conn", "connz", "conns"}
	conns.RunE = c.withWatcher(c.reportConnections)
	cmdAddTags(conns, "scope:system", "impact:ro")
	addArg(conns, "limit", "Limit the responses to a certain amount of servers", false, "int")
	conns.Flags().StringVar(&c.account, "account", "", "Limit report to a specific account")
	conns.Flags().Var(newEnumValue(&c.sort, "subs", "in-bytes", "out-bytes", "in-msgs", "out-msgs", "uptime", "cid", "subs"), "sort", "Sort by a specific property (in-bytes,out-bytes,in-msgs,out-msgs,uptime,cid,subs)")
	conns.Flags().IntVar(&c.topk, "top", 1000, "Limit results to the top results")
	conns.Flags().StringVar(&c.subject, "subject", "", "Limits responses only to those connections with matching subscription interest")
	conns.Flags().StringVar(&c.user, "username", "", "Limits responses only to those connections for a specific authentication username")
	conns.Flags().Var(newEnumValue(&c.stateFilter, "open", "open", "closed", "all"), "state", "Limits responses only to those connections that are in a specific state (open, closed, all)")
	conns.Flags().StringVar(&c.filterReason, "closed-reason", "", "Filter results based on a closed reason")
	flagPlaceholder(conns, "closed-reason", "REASON")
	conns.Flags().StringVar(&c.filterExpression, "filter", "", "Expression based filter for connections")
	addFilterOpts(conns)
	conns.Flags().StringVar(&c.archivePath, "archive", "", "Read data from an archive file")
	conns.Flags().BoolVarP(&c.json, "json", "j", false, "Produce JSON output")

	cpu := addCommand(report, "cpu", "Report on CPU usage")
	cpu.RunE = c.withWatcher(c.reportCPU)
	cmdAddTags(cpu, "scope:system", "impact:ro")
	addArg(cpu, "limit", "Limit the responses to a certain amount of servers", false, "int")
	addFilterOpts(cpu)
	cpu.Flags().StringVar(&c.archivePath, "archive", "", "Read data from an archive file")
	cpu.Flags().BoolVarP(&c.json, "json", "j", false, "Produce JSON output")

	gateways := addCommand(report, "gateways", "Repost on Gateway (Super Cluster) connections")
	gateways.Aliases = []string{"super", "gateway"}
	gateways.RunE = c.withWatcher(c.reportGateway)
	cmdAddTags(gateways, "scope:system", "impact:ro")
	addArg(gateways, "limit", "Limit the responses to a certain amount of servers", false, "int")
	gateways.Flags().StringVar(&c.gatewayName, "filter-name", "", "Limits responses to a certain name")
	gateways.Flags().Var(newEnumValue(&c.sort, "cluster", "server", "cluster"), "sort", "Sorts by a specific property (server,cluster)")
	addFilterOpts(gateways)
	gateways.Flags().StringVar(&c.archivePath, "archive", "", "Read data from an archive file")

	health := addCommand(report, "health", "Report on Server health")
	health.RunE = c.withWatcher(c.reportHealth)
	cmdAddTags(health, "scope:system", "impact:ro")
	addArg(health, "limit", "Limit the responses to a certain amount of servers", false, "int")
	health.Flags().BoolVarP(&c.jsEnabled, "js-enabled", "J", false, "Checks that JetStream should be enabled on all servers")
	health.Flags().BoolVarP(&c.jsServerOnly, "server-only", "S", false, "Restricts the health check to the JetStream server only, do not check streams and consumers")
	health.Flags().StringVar(&c.account, "account", "", "Check only a specific Account")
	health.Flags().StringVar(&c.stream, "stream", "", "Check only a specific Stream")
	health.Flags().StringVar(&c.consumer, "consumer", "", "Check only a specific Consumer")
	addFilterOpts(health)
	health.Flags().StringVar(&c.archivePath, "archive", "", "Read data from an archive file")

	jsz := addCommand(report, "jetstream", "Report on JetStream activity")
	jsz.Aliases = []string{"jsz", "js"}
	jsz.RunE = c.withWatcher(c.reportJetStream)
	cmdAddTags(jsz, "scope:system", "impact:ro")
	addArg(jsz, "limit", "Limit the responses to a certain amount of servers", false, "int")
	jsz.Flags().StringVar(&c.account, "account", "", "Produce the report for a specific account")
	jsz.Flags().Var(newEnumValue(&c.sort, "cluster", "name", "cluster", "streams", "consumers", "msgs", "mbytes", "bytes", "mem", "file", "store", "api", "err"), "sort", "Sort by a specific property (name,cluster,streams,consumers,msgs,mbytes,mem,file,api,err")
	negatableBoolVar(jsz, &c.compact, "compact", true, "Compact server names")
	jsz.Flags().BoolVarP(&c.reportLeaderDistrib, "leaders", "l", false, "Show details about cluster leaders")
	addFilterOpts(jsz)
	jsz.Flags().StringVar(&c.archivePath, "archive", "", "Read data from an archive file")

	leafs := addCommand(report, "leafnodes", "Report on Leafnode connections")
	leafs.Aliases = []string{"leaf", "leafz"}
	leafs.RunE = c.withWatcher(c.reportLeafs)
	cmdAddTags(leafs, "scope:system", "impact:ro")
	addArg(leafs, "limit", "Limit the responses to a certain amount of servers", false, "int")
	leafs.Flags().StringVar(&c.account, "account", "", "Produce the report for a specific account")
	leafs.Flags().Var(newEnumValue(&c.sort, "", "server", "name", "account", "subs", "in-bytes", "out-bytes", "in-msgs", "out-msgs"), "sort", "Sort by a specific property (server,name,account,subs,in-bytes,out-bytes,in-msgs,out-msgs)")
	addFilterOpts(leafs)
	leafs.Flags().StringVar(&c.archivePath, "archive", "", "Read data from an archive file")

	mem := addCommand(report, "mem", "Report on Memory usage")
	mem.RunE = c.withWatcher(c.reportMem)
	cmdAddTags(mem, "scope:system", "impact:ro")
	addArg(mem, "limit", "Limit the responses to a certain amount of servers", false, "int")
	addFilterOpts(mem)
	mem.Flags().StringVar(&c.archivePath, "archive", "", "Read data from an archive file")
	mem.Flags().BoolVarP(&c.json, "json", "j", false, "Produce JSON output")

	routes := addCommand(report, "routes", "Report on Route (Cluster) connections")
	routes.Aliases = []string{"route"}
	routes.RunE = c.withWatcher(c.reportRoute)
	cmdAddTags(routes, "scope:system", "impact:ro")
	addArg(routes, "limit", "Limit the responses to a certain amount of servers", false, "int")
	routes.Flags().Var(newEnumValue(&c.sort, "", "server", "cluster", "name", "account", "subs", "in-bytes", "out-bytes"), "sort", "Sort by a specific property (server,cluster,name,account,subs,in-bytes,out-bytes)")
	addFilterOpts(routes)
	routes.Flags().StringVar(&c.archivePath, "archive", "", "Read data from an archive file")

	reportCmd := addCommand(report, "downgrade", "List assets incompatible with the specified API level")
	reportCmd.RunE = c.downgradeCheckAction
	cmdAddTags(reportCmd, "scope:system", "impact:ro")
	addArg(reportCmd, "api", "Target API level to check compatibility against", true, "uint")
	reportCmd.Flags().BoolVar(&c.json, "json", false, "Output the downgrade report in JSON format")
	reportCmd.Flags().BoolVar(&c.all, "all", false, "Include consumers whose associated streams are incompatible with the selected API level")
	reportCmd.Flags().StringVar(&c.archivePath, "archive", "", "Read data from an archive file")
}

func (c *SrvReportCmd) withWatcher(fn func(*cobra.Command, []string) error) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if c.archivePath == "" {
			nc, _, err := prepareHelper("", natsOpts()...)
			if err != nil {
				return err
			}
			c.nc = nc
		}

		if c.archivePath != "" && c.watchInterval > 0 {
			return fmt.Errorf("--watch is not supported when using --archive")
		}

		if c.watchInterval <= 0 {
			return fn(cmd, args)
		}

		tick := time.NewTicker(time.Second * time.Duration(c.watchInterval))
		ctx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
		defer cancel()

		if err := fn(cmd, args); err != nil {
			return err
		}

		for {
			select {
			case <-tick.C:
				if err := fn(cmd, args); err != nil {
					return err
				}
			case <-ctx.Done():
				return nil
			}
		}
	}
}

func (c *SrvReportCmd) dataSource() (serverdata.Source, error) {
	if c.archivePath != "" {
		return serverdata.NewAuditArchive(c.archivePath)
	}
	return serverdata.NewLive(c.nc, func(req any, subj string, waitFor int, nc *nats.Conn) ([][]byte, error) {
		return serverdata.DoReq(ctx, req, subj, waitFor, nc, opts().Timeout, traceLogger())
	}, c.waitFor)
}

// bindLimitArg binds the optional "limit" positional argument at index idx to c.waitFor.
func (c *SrvReportCmd) bindLimitArg(args []string, idx int) error {
	if v := argValue(args, idx); v != "" {
		i, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		c.waitFor = i
	}
	return nil
}

func (c *SrvReportCmd) reportLeafs(_ *cobra.Command, args []string) error {
	if err := c.bindLimitArg(args, 0); err != nil {
		return err
	}

	src, err := c.dataSource()
	if err != nil {
		return err
	}
	defer src.Close()

	responses, err := src.Leafz(server.LeafzEventOptions{
		LeafzOptions:       server.LeafzOptions{Account: c.account},
		EventFilterOptions: c.reqFilter(),
	})
	if err != nil {
		return err
	}

	type leaf struct {
		server *server.ServerInfo
		leafs  *server.LeafInfo
	}

	var leafs []*leaf
	for _, s := range responses {
		if s.Error != nil {
			return fmt.Errorf("%v", s.Error.Error())
		}

		for _, l := range s.Data.Leafs {
			leafs = append(leafs, &leaf{
				server: s.Server,
				leafs:  l,
			})
		}
	}

	sort.Slice(leafs, func(i, j int) bool {
		switch c.sort {
		case "name":
			return c.boolReverse(leafs[i].leafs.Name < leafs[j].leafs.Name)
		case "account":
			return c.boolReverse(leafs[i].leafs.Account < leafs[j].leafs.Account)
		case "subs":
			return c.boolReverse(leafs[i].leafs.NumSubs < leafs[j].leafs.NumSubs)
		case "in-bytes":
			return c.boolReverse(leafs[i].leafs.InBytes < leafs[j].leafs.InBytes)
		case "out-bytes":
			return c.boolReverse(leafs[i].leafs.OutBytes < leafs[j].leafs.OutBytes)
		case "in-msgs":
			return c.boolReverse(leafs[i].leafs.InMsgs < leafs[j].leafs.InMsgs)
		case "out-msgs":
			return c.boolReverse(leafs[i].leafs.OutMsgs < leafs[j].leafs.OutMsgs)
		default:
			return c.boolReverse(leafs[i].server.Name < leafs[j].server.Name)
		}
	})

	tbl := iu.NewTableWriterf(opts(), "Leafnode Report")
	tbl.AddHeaders("Server", "Name", "Account", "Address", "RTT", "Msgs In", "Msgs Out", "Bytes In", "Bytes Out", "Subs", "Compressed", "Spoke")

	for _, lz := range leafs {
		acct := lz.leafs.Account
		if len(acct) > 23 {
			acct = fmt.Sprintf("%s...%s", acct[0:10], acct[len(acct)-10:])
		}

		tbl.AddRow(
			lz.server.Name,
			lz.leafs.Name,
			acct,
			fmt.Sprintf("%s:%d", lz.leafs.IP, lz.leafs.Port),
			lz.leafs.RTT,
			f(lz.leafs.InMsgs),
			f(lz.leafs.OutMsgs),
			fiBytes(uint64(lz.leafs.InBytes)),
			fiBytes(uint64(lz.leafs.OutBytes)),
			f(lz.leafs.NumSubs),
			f(lz.leafs.Compression),
			f(lz.leafs.IsSpoke),
		)
	}

	fmt.Println(tbl.Render())

	return nil
}

func (c *SrvReportCmd) parseRtt(rtt string, crit time.Duration) string {
	d, err := time.ParseDuration(rtt)
	if err != nil {
		return rtt
	}

	if d < crit {
		return f(d)
	}

	return color.RedString(f(d))
}

func (c *SrvReportCmd) reportHealth(_ *cobra.Command, args []string) error {
	if err := c.bindLimitArg(args, 0); err != nil {
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
			Account:       c.account,
			Stream:        c.stream,
			Consumer:      c.consumer,
			Details:       true,
		},
		EventFilterOptions: c.reqFilter(),
	})
	if err != nil {
		return err
	}

	var servers []server.ServerAPIHealthzResponse
	for _, s := range responses {
		if s.Error != nil {
			return fmt.Errorf("%v", s.Error.Error())
		}

		servers = append(servers, *s)
	}

	sort.Slice(servers, func(i, j int) bool {
		return c.boolReverse(servers[i].Server.Name < servers[j].Server.Name)
	})

	tbl := iu.NewTableWriterf(opts(), "Health Report")
	tbl.AddHeaders("Server", "Cluster", "Domain", "Status", "Type", "Error")

	var ok, notok, totalErrors int
	totalClusters := map[string]struct{}{}

	for _, srv := range servers {
		tbl.AddRow(
			srv.Server.Name,
			srv.Server.Cluster,
			srv.Server.Domain,
			fmt.Sprintf("%s (%d)", srv.Data.Status, srv.Data.StatusCode),
		)

		if srv.Data.StatusCode == 200 {
			ok += 1
		} else {
			notok += 1
		}

		totalClusters[srv.Server.Cluster] = struct{}{}

		ecnt := len(srv.Data.Errors)
		if ecnt == 0 {
			continue
		}

		totalErrors += ecnt

		show := ecnt
		if ecnt > 10 {
			show = 9
		}

		for _, errStatus := range srv.Data.Errors[0:show] {
			tbl.AddRow(
				"", "", "", "",
				errStatus.Type.String(),
				errStatus.Error,
			)
		}
		if show != ecnt {
			tbl.AddRow("", "", "", fmt.Sprintf("%d more errors", ecnt-show))
		}
	}

	//tbl.AddSeparator()
	tbl.AddFooter(f(len(servers)), f(len(totalClusters)), "", f(fmt.Sprintf("ok: %d / err: %d", ok, notok)), "", f(totalErrors))

	if c.watchInterval > 0 {
		iu.ClearScreen()
	}
	fmt.Println(tbl.Render())

	return nil
}

func (c *SrvReportCmd) reportGateway(_ *cobra.Command, args []string) error {
	if err := c.bindLimitArg(args, 0); err != nil {
		return err
	}

	src, err := c.dataSource()
	if err != nil {
		return err
	}
	defer src.Close()

	responses, err := src.Gatewayz(server.GatewayzEventOptions{
		EventFilterOptions: c.reqFilter(),
		GatewayzOptions: server.GatewayzOptions{
			Name:     c.gatewayName,
			Accounts: true,
		},
	})
	if err != nil {
		return err
	}

	var gateways []server.ServerAPIGatewayzResponse
	for _, g := range responses {
		if g.Error != nil {
			return fmt.Errorf("%v", g.Error.Error())
		}

		gateways = append(gateways, *g)
	}

	sort.Slice(gateways, func(i, j int) bool {
		switch c.sort {
		case "cluster":
			if gateways[i].Server.Cluster == gateways[j].Server.Cluster {
				return c.boolReverse(gateways[i].Server.Name < gateways[j].Server.Name)
			}
			return c.boolReverse(gateways[i].Server.Cluster < gateways[j].Server.Cluster)

		case "server":
			if gateways[i].Server.Name == gateways[j].Server.Name {
				return c.boolReverse(gateways[i].Server.Cluster < gateways[j].Server.Cluster)
			}
			return c.boolReverse(gateways[i].Server.Name < gateways[j].Server.Name)

		default:
			if gateways[i].Server.Name == gateways[j].Server.Name {
				return c.boolReverse(gateways[i].Server.Cluster < gateways[j].Server.Cluster)
			}
			return c.boolReverse(gateways[i].Server.Name < gateways[j].Server.Name)
		}
	})

	tbl := iu.NewTableWriterf(opts(), "Super Cluster Report")
	tbl.AddHeaders("Server", "Name", "Port", "Kind", "Connection", "ID", "Uptime", "RTT", "Bytes", "Accounts")

	var lastServer, lastName, lastDirection string
	var totalBytes int64
	var totalServers, totalGateways int
	totalClusters := map[string]struct{}{}

	for _, g := range gateways {
		sname := g.Server.Name
		totalServers++

		if sname == lastServer {
			sname = ""
		}
		lastServer = g.Server.Name

		cname := g.Server.Cluster
		if cname == lastName {
			cname = ""
		}
		lastName = g.Server.Name
		totalClusters[cname] = struct{}{}

		tbl.AddRow(
			sname,
			cname,
			g.Data.Port,
			"", "", "", "", "", "", "",
		)

		lastDirection = ""
		for gname, conns := range g.Data.InboundGateways {
			for _, conn := range conns {
				totalGateways++
				direction := "Inbound"
				if direction == lastDirection {
					direction = ""
				} else {
					lastDirection = "Inbound"
				}

				uptime := conn.Connection.Uptime
				if d, err := time.ParseDuration(uptime); err == nil {
					uptime = f(d)
				}

				totalBytes += conn.Connection.InBytes
				tbl.AddRow(
					"", "", "",
					direction,
					fmt.Sprintf("%s %s:%d", gname, conn.Connection.IP, conn.Connection.Port),
					fmt.Sprintf("gid:%d", conn.Connection.Cid),
					uptime,
					c.parseRtt(conn.Connection.RTT, 300*time.Millisecond),
					fiBytes(uint64(conn.Connection.InBytes)),
					f(len(conn.Accounts)),
				)
			}
		}

		for gname, conn := range g.Data.OutboundGateways {
			totalGateways++
			direction := "Outbound"
			if direction == lastDirection {
				direction = ""
			} else {
				lastDirection = "Outbound"
			}

			totalBytes += conn.Connection.OutBytes
			tbl.AddRow(
				"", "", "",
				direction,
				fmt.Sprintf("%s %s:%d", gname, conn.Connection.IP, conn.Connection.Port),
				fmt.Sprintf("gid:%d", conn.Connection.Cid),
				conn.Connection.Uptime,
				c.parseRtt(conn.Connection.RTT, 300*time.Millisecond),
				fiBytes(uint64(conn.Connection.OutBytes)),
				f(len(conn.Accounts)),
			)
		}
	}

	tbl.AddFooter(f(totalServers), len(totalClusters), "", "", f(totalGateways), "", "", "", fiBytes(uint64(totalBytes)), "")

	if c.watchInterval > 0 {
		iu.ClearScreen()
	}
	fmt.Println(tbl.Render())

	return nil
}

func (c *SrvReportCmd) reportRoute(_ *cobra.Command, args []string) error {
	if err := c.bindLimitArg(args, 0); err != nil {
		return err
	}

	src, err := c.dataSource()
	if err != nil {
		return err
	}
	defer src.Close()

	responses, err := src.Routez(server.RoutezEventOptions{
		EventFilterOptions: c.reqFilter(),
	})
	if err != nil {
		return err
	}

	type routeData struct {
		server *server.ServerInfo
		routes *server.RouteInfo
	}

	routes := []routeData{}
	for _, r := range responses {
		if r.Error != nil {
			return fmt.Errorf("%v", r.Error.Error())
		}

		if len(r.Data.Routes) > 0 {
			for _, route := range r.Data.Routes {
				routes = append(routes, routeData{r.Server, route})
			}
		}
	}

	sort.Slice(routes, func(i, j int) bool {
		a, b := routes[i], routes[j]

		var wasSorted bool
		switch c.sort {
		case "cluster":
			if a.server.Cluster != b.server.Cluster {
				wasSorted = a.server.Cluster < b.server.Cluster
			}
		case "name":
			if a.routes.RemoteName != b.routes.RemoteName {
				wasSorted = a.routes.RemoteName < b.routes.RemoteName
			}
		case "account", "acct":
			if a.routes.Account != b.routes.Account {
				wasSorted = a.routes.Account < b.routes.Account
			}
		case "subs":
			if a.routes.NumSubs != b.routes.NumSubs {
				wasSorted = a.routes.NumSubs < b.routes.NumSubs
			}
		case "in-bytes":
			if a.routes.InBytes != b.routes.InBytes {
				wasSorted = a.routes.InBytes < b.routes.InBytes
			}
		case "out-bytes":
			if a.routes.OutBytes != b.routes.OutBytes {
				wasSorted = a.routes.OutBytes < b.routes.OutBytes
			}
		case "server":
			if a.server.Name != b.server.Name {
				wasSorted = a.server.Name < b.server.Name
			}
		default:
			if a.server.Name != b.server.Name {
				wasSorted = a.server.Name < b.server.Name
			}
		}

		// Enforce consistent ordering when primary values are equal
		if !wasSorted && !c.boolReverse(wasSorted) {
			switch {
			case a.server.Name != b.server.Name:
				wasSorted = a.server.Name < b.server.Name
			case a.server.Cluster != b.server.Cluster:
				wasSorted = a.server.Cluster < b.server.Cluster
			case a.routes.RemoteName != b.routes.RemoteName:
				wasSorted = a.routes.RemoteName < b.routes.RemoteName
			default:
				wasSorted = a.routes.Account < b.routes.Account
			}
		}

		return c.boolReverse(wasSorted)
	})

	tbl := iu.NewTableWriterf(opts(), "Cluster Report")
	tbl.AddHeaders("Server", "Cluster", "Name", "Account", "Address", "ID", "Uptime", "RTT", "Subs", "Bytes In", "Bytes Out")

	var lastServer, lastCluster, lastRemote string
	var subs, bytesIn, bytesOut, totalRoutes int64
	totalServers := map[string]struct{}{}
	totalClusters := map[string]struct{}{}

	for _, route := range routes {
		r := route.routes

		totalServers[route.server.Name] = struct{}{}
		totalClusters[route.server.Cluster] = struct{}{}

		totalRoutes++

		uptime := route.server.Time.Sub(r.Start)
		sname := route.server.Name
		if sname == lastServer {
			sname = ""
		} else {
			lastRemote = ""
			lastCluster = ""
		}
		lastServer = route.server.Name

		cname := route.server.Cluster
		if cname == lastCluster {
			cname = ""
		}
		lastCluster = route.server.Cluster

		rname := r.RemoteName
		if rname == lastRemote {
			rname = ""
		}
		lastRemote = r.RemoteName

		acct := r.Account
		if len(acct) > 23 {
			acct = fmt.Sprintf("%s...%s", acct[0:10], acct[len(acct)-10:])
		}
		subs += int64(r.NumSubs)
		bytesIn += r.InBytes
		bytesOut += r.OutBytes

		tbl.AddRow(
			sname,
			cname,
			rname,
			acct,
			fmt.Sprintf("%s:%d", r.IP, r.Port),
			fmt.Sprintf("rid:%d", r.Rid),
			f(uptime),
			c.parseRtt(r.RTT, 100*time.Millisecond),
			f(r.NumSubs),
			fiBytes(uint64(r.InBytes)),
			fiBytes(uint64(r.OutBytes)),
		)
	}

	tbl.AddFooter(f(len(totalServers)), f(len(totalClusters)), "", "", f(totalRoutes), "", "", "", f(subs), fiBytes(uint64(bytesIn)), fiBytes(uint64(bytesOut)))

	if c.watchInterval > 0 {
		iu.ClearScreen()
	}
	fmt.Println(tbl.Render())

	return nil
}

func (c *SrvReportCmd) reportMem(_ *cobra.Command, args []string) error {
	if err := c.bindLimitArg(args, 0); err != nil {
		return err
	}
	return c.reportCpuOrMem(true)
}

func (c *SrvReportCmd) reportCPU(_ *cobra.Command, args []string) error {
	if err := c.bindLimitArg(args, 0); err != nil {
		return err
	}
	return c.reportCpuOrMem(false)
}

func (c *SrvReportCmd) reportCpuOrMem(mem bool) error {
	src, err := c.dataSource()
	if err != nil {
		return err
	}
	defer src.Close()

	responses, err := src.Statz(server.StatszEventOptions{EventFilterOptions: c.reqFilter()})
	if err != nil {
		return err
	}

	usage := map[string]float64{}

	for _, sr := range responses {
		if mem {
			usage[sr.Server.Name] = float64(sr.Stats.Mem)
		} else {
			usage[sr.Server.Name] = sr.Stats.CPU
		}
	}

	if c.json {
		return iu.PrintJSON(usage)
	}

	width := iu.ProgressWidth() / 2
	if width > 30 {
		width = 30
	}

	if mem {
		return iu.BarGraph(os.Stdout, usage, "Memory Usage", width, true)
	}

	if c.watchInterval > 0 {
		iu.ClearScreen()
	}
	return iu.BarGraph(os.Stdout, usage, "CPU Usage", width, false)
}

func (c *SrvReportCmd) reportJetStream(_ *cobra.Command, args []string) error {
	if err := c.bindLimitArg(args, 0); err != nil {
		return err
	}

	jszOpts := server.JSzOptions{RaftGroups: true}
	if c.account != "" {
		jszOpts.Account = c.account
		jszOpts.Streams = true
		jszOpts.Consumer = true
		jszOpts.Limit = 10000
	}

	src, err := c.dataSource()
	if err != nil {
		return err
	}
	defer src.Close()

	jszResponses, err := src.Jsz(server.JszEventOptions{JSzOptions: jszOpts, EventFilterOptions: c.reqFilter()})
	if err != nil {
		return err
	}

	var (
		names               []string
		apiTotal            uint64
		pendingTotal        int
		memoryTotal         uint64
		storeTotal          uint64
		consumersTotal      int
		streamsTotal        int
		bytesTotal          uint64
		msgsTotal           uint64
		clusters            []*server.MetaClusterInfo
		consumerLeaderStats = make(map[string]*raftLeader)
		streamLeaderStats   = make(map[string]*raftLeader)
		expectedClusterSize int
		renderAssetLeaders  bool
	)

	// TODO: remove after 2.12 is out
	renderDomain := false
	for _, response := range jszResponses {
		if response.Data.Config.Domain != "" {
			renderDomain = true
		}
	}

	sort.Slice(jszResponses, func(i, j int) bool {
		switch c.sort {
		case "name":
			return c.boolReverse(jszResponses[i].Server.Name < jszResponses[j].Server.Name)
		case "streams":
			if jszResponses[i].Data.Streams != jszResponses[j].Data.Streams {
				return c.boolReverse(jszResponses[i].Data.Streams < jszResponses[j].Data.Streams)
			}
			return c.boolReverse(jszResponses[i].Server.Name < jszResponses[j].Server.Name)

		case "consumers":
			if jszResponses[i].Data.Consumers != jszResponses[j].Data.Consumers {
				return c.boolReverse(jszResponses[i].Data.Consumers < jszResponses[j].Data.Consumers)
			}
			return c.boolReverse(jszResponses[i].Server.Name < jszResponses[j].Server.Name)

		case "msgs":
			return c.boolReverse(jszResponses[i].Data.Messages < jszResponses[j].Data.Messages)
		case "mbytes", "bytes":
			return c.boolReverse(jszResponses[i].Data.Bytes < jszResponses[j].Data.Bytes)
		case "mem":
			return c.boolReverse(jszResponses[i].Data.JetStreamStats.Memory < jszResponses[j].Data.JetStreamStats.Memory)
		case "store", "file":
			return c.boolReverse(jszResponses[i].Data.JetStreamStats.Store < jszResponses[j].Data.JetStreamStats.Store)
		case "api":
			return c.boolReverse(jszResponses[i].Data.JetStreamStats.API.Total < jszResponses[j].Data.JetStreamStats.API.Total)
		default:
			if jszResponses[i].Server.Cluster != jszResponses[j].Server.Cluster {
				return c.boolReverse(jszResponses[i].Server.Cluster < jszResponses[j].Server.Cluster)
			}
			return c.boolReverse(jszResponses[i].Server.Name < jszResponses[j].Server.Name)
		}
	})

	if len(jszResponses) == 0 {
		if c.archivePath != "" {
			return fmt.Errorf("no JetStream data found in %s", c.archivePath)
		}
		return fmt.Errorf("no results received, ensure the account used has system privileges and appropriate permissions")
	}

	// here so it's after the sort
	for _, js := range jszResponses {
		names = append(names, js.Server.Name)
	}
	var cNames []string
	if c.compact {
		cNames = iu.CompactStrings(names)
	} else {
		cNames = names
	}

	var table *iu.Table
	if c.account != "" {
		table = iu.NewTableWriterf(opts(), "JetStream Summary for Account %s", c.account)
	} else {
		table = iu.NewTableWriter(opts(), "JetStream Summary")
	}

	hdrs := []any{"Server", "Cluster"}
	if renderDomain {
		hdrs = append(hdrs, "Domain")
	}
	hdrs = append(hdrs, "Streams", "Consumers", "Messages", "Bytes", "Memory", "File", "API Req", "Pending")
	table.AddHeaders(hdrs...)

	for i, js := range jszResponses {
		jss := js.Data.JetStreamStats
		var acc *server.AccountDetail
		var doAccountStats bool

		if c.account != "" && len(js.Data.AccountDetails) == 1 {
			acc = js.Data.AccountDetails[0]
			jss = acc.JetStreamStats
			doAccountStats = true
		}

		apiTotal += jss.API.Total
		memoryTotal += jss.Memory
		storeTotal += jss.Store

		rPending := 0
		rStreams := 0
		rConsumers := 0
		rMessages := uint64(0)
		rBytes := uint64(0)

		if doAccountStats {
			rBytes = acc.Memory + acc.Store
			bytesTotal += rBytes
			rStreams = len(acc.Streams)
			streamsTotal += rStreams

			for _, sd := range acc.Streams {
				consumersTotal += sd.State.Consumers
				rConsumers += sd.State.Consumers
				msgsTotal += sd.State.Msgs
				rMessages += sd.State.Msgs
				if sd.Cluster != nil && sd.Cluster.Leader == js.Server.Name {
					if _, ok := streamLeaderStats[sd.Cluster.Leader]; ok {
						streamLeaderStats[sd.Cluster.Leader].groups++
					} else {
						streamLeaderStats[sd.Cluster.Leader] = &raftLeader{name: sd.Cluster.Leader, cluster: sd.Cluster.Name, groups: 1}
					}
				}

				for _, consumer := range sd.Consumer {
					if consumer.Cluster != nil && consumer.Cluster.Leader == js.Server.Name {
						if _, ok := consumerLeaderStats[sd.Cluster.Leader]; ok {
							consumerLeaderStats[sd.Cluster.Leader].groups++
						} else {
							consumerLeaderStats[sd.Cluster.Leader] = &raftLeader{name: sd.Cluster.Leader, cluster: sd.Cluster.Name, groups: 1}
						}
					}
				}
			}
		} else {
			consumersTotal += js.Data.Consumers
			rConsumers = js.Data.Consumers
			streamsTotal += js.Data.Streams
			rStreams = js.Data.Streams
			bytesTotal += js.Data.Bytes
			rBytes = js.Data.Bytes
			msgsTotal += js.Data.Messages
			rMessages = js.Data.Messages
			if js.Data.StreamsLeader > 0 || js.Data.ConsumersLeader > 0 {
				consumerLeaderStats[js.Server.Name] = &raftLeader{name: js.Server.Name, cluster: js.Server.Cluster, groups: js.Data.ConsumersLeader}
				streamLeaderStats[js.Server.Name] = &raftLeader{name: js.Server.Name, cluster: js.Server.Cluster, groups: js.Data.StreamsLeader}
			}
		}

		leader := ""
		if js.Data.Meta != nil {
			if js.Data.Meta.Leader == js.Server.Name {
				leader = "*"
				clusters = append(clusters, js.Data.Meta)
			}
			if expectedClusterSize < js.Data.Meta.Size {
				expectedClusterSize = js.Data.Meta.Size
			}
			rPending = js.Data.Meta.Pending
			pendingTotal += rPending
		}

		row := []any{cNames[i] + leader, js.Server.Cluster}
		if renderDomain {
			row = append(row, js.Data.Config.Domain)
		}

		row = append(row,
			f(rStreams),
			f(rConsumers),
			f(rMessages),
			humanize.IBytes(rBytes),
			humanize.IBytes(jss.Memory),
			humanize.IBytes(jss.Store),
			f(jss.API.Total),
			rPending,
		)

		table.AddRow(row...)

		if !renderAssetLeaders && consumerLeaderStats[js.Server.Name] != nil || streamLeaderStats[js.Server.Name] != nil {
			renderAssetLeaders = true
		}
	}

	row := []any{"", ""}
	if renderDomain {
		row = append(row, "")
	}
	row = append(row, f(streamsTotal), f(consumersTotal), f(msgsTotal), humanize.IBytes(bytesTotal), humanize.IBytes(memoryTotal), humanize.IBytes(storeTotal), f(apiTotal), pendingTotal)
	table.AddFooter(row...)

	if c.watchInterval > 0 {
		iu.ClearScreen()
	}
	fmt.Print(table.Render())
	fmt.Println()

	switch {
	case c.isFiltered():
	case expectedClusterSize == 0:
	case len(jszResponses) > 0 && len(clusters) == 0:
		fmt.Println()
		fmt.Printf("WARNING: No cluster meta leader found. The cluster expects %d nodes but only %d responded. JetStream operation requires at least %d up nodes.", expectedClusterSize, len(jszResponses), expectedClusterSize/2+1)
		fmt.Println()
	default:
		for i, cluster := range clusters {
			cluster.Replicas = append(cluster.Replicas, &server.PeerInfo{
				Name:    cluster.Leader,
				Current: true,
				Offline: false,
				Active:  0,
				Lag:     0,
			})

			sort.Slice(cluster.Replicas, func(i, j int) bool {
				return cluster.Replicas[i].Name < cluster.Replicas[j].Name
			})

			names := []string{}
			for _, r := range cluster.Replicas {
				names = append(names, r.Name)
			}
			if c.compact {
				cNames = iu.CompactStrings(names)
			} else {
				cNames = names
			}

			header := "RAFT Meta Group Information - Lead cluster: " + cluster.Name
			table := iu.NewTableWriterf(opts(), "%s", header)

			table.AddHeaders("Connection Name", "ID", "Leader", "Current", "Online", "Active", "Lag")
			for i, replica := range cluster.Replicas {
				leader := ""
				peer := replica.Peer
				if replica.Name == cluster.Leader {
					leader = "yes"
					peer = cluster.Peer
				}

				online := "true"
				if replica.Offline {
					online = color.New(color.Bold).Sprint("false")
				}

				table.AddRow(cNames[i], peer, leader, replica.Current, online, f(replica.Active), f(replica.Lag))
			}

			if i >= 1 {
				fmt.Println()
			}
			fmt.Print(table.Render())
		}
	}

	if c.reportLeaderDistrib {
		fmt.Println()
		if !renderAssetLeaders {
			fmt.Println("No JetStream asset leader data reported")
		} else {
			renderRaftLeaders(streamLeaderStats, "Stream Leaders")
			renderRaftLeaders(consumerLeaderStats, "Consumer Leaders")
		}
	}

	return nil
}

func (c *SrvReportCmd) reportAccount(_ *cobra.Command, args []string) error {
	c.account = argValue(args, 0)
	if err := c.bindLimitArg(args, 1); err != nil {
		return err
	}

	connz, err := c.getConnz(0, c.nc)
	if err != nil {
		return err
	}

	if len(connz) == 0 {
		return fmt.Errorf("did not get results from any servers")
	}

	if c.account != "" {
		accounts := c.accountInfo(connz)
		if len(accounts) != 1 {
			return fmt.Errorf("received results for multiple accounts, expected %v", c.account)
		}

		account, ok := accounts[c.account]
		if !ok {
			return fmt.Errorf("did not receive any results for account %s", c.account)
		}

		if c.json {
			iu.PrintJSON(account)
			return nil
		}

		if len(account.ConnInfo) > 0 {
			report := account.ConnInfo
			c.renderConnections(report)
		}
		return nil
	}

	accountsMap := c.accountInfo(connz)
	var accounts []*srvReportAccountInfo
	for _, v := range accountsMap {
		accounts = append(accounts, v)
	}

	sort.Slice(accounts, func(i int, j int) bool {
		switch c.sort {
		case "in-bytes":
			return c.boolReverse(accounts[i].InBytes < accounts[j].InBytes)
		case "out-bytes":
			return c.boolReverse(accounts[i].OutBytes < accounts[j].OutBytes)
		case "in-msgs":
			return c.boolReverse(accounts[i].InMsgs < accounts[j].InMsgs)
		case "out-msgs":
			return c.boolReverse(accounts[i].OutMsgs < accounts[j].OutMsgs)
		case "conns":
			return c.boolReverse(accounts[i].Connections < accounts[j].Connections)
		default:
			return c.boolReverse(accounts[i].Subs < accounts[j].Subs)
		}
	})

	if c.topk > 0 && c.topk < len(accounts) {
		if c.reverse {
			accounts = accounts[0:c.topk]
		} else {
			accounts = accounts[len(accounts)-c.topk:]
		}
	}

	if c.json {
		iu.PrintJSON(accounts)
		return nil
	}

	table := iu.NewTableWriterf(opts(), "%d Accounts Overview", len(accounts))
	table.AddHeaders("Account", "Connections", "In Msgs", "Out Msgs", "In Bytes", "Out Bytes", "Subs")

	for _, acct := range accounts {
		table.AddRow(acct.Account, f(acct.Connections), f(acct.InMsgs), f(acct.OutMsgs), humanize.IBytes(uint64(acct.InBytes)), humanize.IBytes(uint64(acct.OutBytes)), f(acct.Subs))
	}

	if c.watchInterval > 0 {
		iu.ClearScreen()
	}
	fmt.Print(table.Render())

	return nil
}

func (c *SrvReportCmd) accountInfo(connz connzList) map[string]*srvReportAccountInfo {
	result := make(map[string]*srvReportAccountInfo)

	for _, conn := range connz {
		for _, info := range conn.Data.Conns {
			account, ok := result[info.Account]
			if !ok {
				result[info.Account] = &srvReportAccountInfo{Account: info.Account}
				account = result[info.Account]
			}

			account.ConnInfo = append(account.ConnInfo, connInfo{info, conn.Server})
			account.Connections++
			account.InBytes += info.InBytes
			account.OutBytes += info.OutBytes
			account.InMsgs += info.InMsgs
			account.OutMsgs += info.OutMsgs
			account.Subs += len(info.Subs)

			// make sure we only store one server info per unique server
			found := false
			for _, s := range account.Server {
				if s.ID == conn.Server.ID {
					found = true
					break
				}
			}
			if !found {
				account.Server = append(account.Server, conn.Server)
			}
		}
	}

	return result
}

type connInfo struct {
	*server.ConnInfo
	Info *server.ServerInfo `json:"server"`
}

func (c *SrvReportCmd) reportConnections(_ *cobra.Command, args []string) error {
	if err := c.bindLimitArg(args, 0); err != nil {
		return err
	}

	connz, err := c.getConnz(0, c.nc)
	if err != nil {
		return err
	}

	if len(connz) == 0 {
		return fmt.Errorf("did not get results from any servers")
	}

	conns := connz.flatConnInfo()

	if c.json {
		iu.PrintJSON(conns)
		return nil
	}

	c.renderConnections(conns)

	return nil
}

func (c *SrvReportCmd) boolReverse(v bool) bool {
	if c.reverse {
		return !v
	}

	return v
}

func (c *SrvReportCmd) sortConnections(conns []connInfo) {
	sort.Slice(conns, func(i int, j int) bool {
		switch c.sort {
		case "in-bytes":
			return c.boolReverse(conns[i].InBytes < conns[j].InBytes)
		case "out-bytes":
			return c.boolReverse(conns[i].OutBytes < conns[j].OutBytes)
		case "in-msgs":
			return c.boolReverse(conns[i].InMsgs < conns[j].InMsgs)
		case "out-msgs":
			return c.boolReverse(conns[i].OutMsgs < conns[j].OutMsgs)
		case "uptime":
			return c.boolReverse(conns[i].Start.After(conns[j].Start))
		case "cid":
			return c.boolReverse(conns[i].Cid < conns[j].Cid)
		default:
			return c.boolReverse(len(conns[i].Subs) < len(conns[j].Subs))
		}
	})
}

func (c *SrvReportCmd) renderConnections(report []connInfo) {
	c.sortConnections(report)

	total := len(report)
	limit := total
	if c.topk > 0 && c.topk <= total {
		limit = c.topk
	}

	table := iu.NewTableWriterf(opts(), "Top %d Connections out of %s by %s", limit, f(total), c.sort)
	showReason := c.stateFilter == "closed" || c.stateFilter == "all"
	headers := []any{"CID", "Name", "Server", "Cluster", "IP", "Account", "Uptime", "In Msgs", "Out Msgs", "In Bytes", "Out Bytes", "Subs"}
	if showReason {
		headers = append(headers, "Reason")
	}

	table.AddHeaders(headers...)

	var oMsgs int64
	var iMsgs int64
	var oBytes int64
	var iBytes int64
	var subs uint32

	type srvInfo struct {
		cluster string
		conns   int
	}
	servers := make(map[string]*srvInfo)
	var serverNames []string

	for i, info := range report {
		name := info.Name
		if len(name) == 0 && len(info.MQTTClient) > 0 {
			name = info.MQTTClient
		}
		if len(name) > 40 {
			name = name[:40] + " .."
		}

		oMsgs += info.OutMsgs
		iMsgs += info.InMsgs
		oBytes += info.OutBytes
		iBytes += info.InBytes
		subs += info.NumSubs

		srvName := info.Info.Name
		cluster := info.Info.Cluster

		srv, ok := servers[srvName]
		if !ok {
			servers[srvName] = &srvInfo{cluster, 0}
			srv = servers[srvName]
			serverNames = append(serverNames, srvName)
		}
		srv.conns++

		acc := info.Account
		if len(info.Account) > 46 {
			acc = info.Account[0:12] + " .."
		}

		cid := fmt.Sprintf("%d", info.Cid)
		if info.Kind != "Client" {
			cid = fmt.Sprintf("%s%d", string(info.Kind[0]), info.Cid)
		}

		if i < limit {
			values := []any{cid, name, srvName, cluster, fmt.Sprintf("%s:%d", info.IP, info.Port), acc, info.Uptime, f(info.InMsgs), f(info.OutMsgs), humanize.IBytes(uint64(info.InBytes)), humanize.IBytes(uint64(info.OutBytes)), f(len(info.Subs))}
			if showReason {
				values = append(values, info.Reason)
			}
			table.AddRow(values...)
		}
	}

	if len(report) > 1 {
		values := []any{"", fmt.Sprintf("Totals for %s connections", humanize.Comma(int64(total))), "", "", "", "", "", f(iMsgs), f(oMsgs), humanize.IBytes(uint64(iBytes)), humanize.IBytes(uint64(oBytes)), f(subs)}
		if showReason {
			values = append(values, "")
		}
		table.AddFooter(values...)
	}

	if c.watchInterval > 0 {
		iu.ClearScreen()
	}
	fmt.Print(table.Render())

	if len(serverNames) > 0 {
		fmt.Println()

		sort.Slice(serverNames, func(i, j int) bool {
			return servers[serverNames[i]].conns < servers[serverNames[j]].conns
		})

		table := iu.NewTableWriterf(opts(), "Connections per server")
		table.AddHeaders("Server", "Cluster", "Connections")
		sort.Slice(serverNames, func(i, j int) bool {
			return servers[serverNames[i]].conns < servers[serverNames[j]].conns
		})

		for _, n := range serverNames {
			table.AddRow(n, servers[n].cluster, servers[n].conns)
		}
		fmt.Print(table.Render())
	}
}

type connzList []*server.ServerAPIConnzResponse

func (c connzList) flatConnInfo() []connInfo {
	var conns []connInfo

	for _, conn := range c {
		for _, c := range conn.Data.Conns {
			conns = append(conns, connInfo{c, conn.Server})
		}
	}

	return conns
}

func (c *SrvReportCmd) getConnz(limit int, nc *nats.Conn) (connzList, error) {
	result := connzList{}
	found := 0

	var program *vm.Program
	var err error
	env := map[string]any{}

	if !c.skipDiscoverClusterSize && c.waitFor == 0 && c.archivePath == "" {
		c.waitFor, err = serverdata.CurrentActiveServers(ctx, nc, opts().Timeout, traceLogger())
		if err != nil {
			return nil, err
		}
	}

	switch {
	case c.filterReason != "" && c.filterExpression != "":
		return nil, fmt.Errorf("cannot filter for closed reason and use a filter expression at the same time")
	case c.filterReason != "":
		c.filterExpression = fmt.Sprintf("lower(Conn.Reason) matches '%s'", c.filterReason)
		fallthrough
	case c.filterExpression != "":
		program, err = expr.Compile(c.filterExpression, expr.Env(map[string]any{}), expr.AsBool(), expr.AllowUndefinedVariables())
		fatalIfError(err, "Invalid expression: %v", err)
	}

	removeFilteredConns := func(co *server.ServerAPIConnzResponse) error {
		conns := make([]*server.ConnInfo, len(co.Data.Conns))
		copy(conns, co.Data.Conns)
		co.Data.Conns = []*server.ConnInfo{}
		srv := iu.StructWithoutOmitEmpty(*co.Server)

		for _, conn := range conns {
			env["server"] = srv
			env["Server"] = co.Server
			env["conn"] = iu.StructWithoutOmitEmpty(*conn)
			env["Conn"] = conn

			// backward compat, the `s` here is a mistake
			env["Conns"] = conn
			env["conns"] = env["conn"]

			out, err := expr.Run(program, env)
			if err != nil {
				fatalIfError(err, "Invalid expression: %v", err)
			}

			should, ok := out.(bool)
			if !ok {
				fatalIfError(err, "expression did not return a boolean")
			}

			if should {
				co.Data.Conns = append(co.Data.Conns, conn)
			}
		}

		return nil
	}

	state := server.ConnOpen
	switch c.stateFilter {
	case "all":
		state = server.ConnAll
	case "closed":
		state = server.ConnClosed
	}

	src, err := c.dataSource()
	if err != nil {
		return nil, err
	}
	defer src.Close()

	offset := 0
	more := false

	connzOpts := server.ConnzOptions{
		Subscriptions:       true,
		SubscriptionsDetail: false,
		Username:            true,
		User:                c.user,
		Account:             c.account,
		State:               state,
		FilterSubject:       c.subject,
		Limit:               1024,
		Offset:              offset,
	}

	responses, err := src.Connz(server.ConnzEventOptions{
		ConnzOptions:       connzOpts,
		EventFilterOptions: c.reqFilter(),
	})
	if err != nil {
		return nil, err
	}

	for _, co := range responses {
		if co.Error != nil {
			return nil, fmt.Errorf("invalid response received: %v", co.Error)
		}
		if co.Data == nil {
			return nil, fmt.Errorf("no data received in response")
		}
		found += len(co.Data.Conns)

		if c.filterExpression != "" {
			err = removeFilteredConns(co)
			if err != nil {
				return nil, err
			}
		}

		if len(co.Data.Conns) > 0 {
			result = append(result, co)
		}
	}

	if limit != 0 && found > limit {
		return result[:limit], nil
	}

	for _, conn := range result {
		if conn.Data.Offset+conn.Data.Limit < conn.Data.Total {
			more = true
		}
	}

	if more && !c.json {
		fmt.Printf("Gathering paged connection information")
	}

	for {
		if !more {
			break
		}

		if limit != 0 && found > limit {
			break
		}

		// Show visual progress if JSON is not requested.
		if !c.json {
			fmt.Print(".")
		}

		offset += 1025
		connzOpts.Offset = offset

		responses, err := src.Connz(server.ConnzEventOptions{
			ConnzOptions:       connzOpts,
			EventFilterOptions: c.reqFilter(),
		})
		if errors.Is(err, nats.ErrNoResponders) {
			return nil, fmt.Errorf("server request failed, ensure the account used has system privileges and appropriate permissions")
		} else if err != nil {
			return nil, err
		}

		more = false

		for _, co := range responses {
			if co.Error != nil {
				return nil, fmt.Errorf("invalid response received: %v", co.Error)
			}
			if co.Data == nil {
				return nil, fmt.Errorf("no data received in response")
			}
			found += len(co.Data.Conns)

			if len(co.Data.Conns) == 0 {
				continue
			}

			if c.filterExpression != "" {
				err = removeFilteredConns(co)
				if err != nil {
					return nil, err
				}
			}

			result = append(result, co)

			if !more && co.Data.Offset+co.Data.Limit < co.Data.Total {
				more = true
			}
		}
	}

	if !c.json {
		fmt.Println()
	}

	if limit > 0 {
		result = result[:limit]
	}

	return result, nil
}

func (c *SrvReportCmd) isFiltered() bool {
	return c.server != "" || len(c.tags) > 0 || c.cluster != ""
}

func (c *SrvReportCmd) reqFilter() server.EventFilterOptions {
	return server.EventFilterOptions{
		Domain:     opts().Config.JSDomain(),
		Name:       c.server,
		Cluster:    c.cluster,
		Tags:       c.tags,
		ExactMatch: true,
	}
}

type jsDowngradeAsset struct {
	Streams   []jsDowngradeStreamAsset   `json:"streams,omitempty"`
	Consumers []jsDowngradeConsumerAsset `json:"consumers,omitempty"`
}

type jsDowngradeStreamAsset struct {
	Account  string `json:"account"`
	Name     string `json:"name"`
	APILevel uint   `json:"api_level"`
}

type jsDowngradeConsumerAsset struct {
	Account  string `json:"account"`
	Name     string `json:"name"`
	Stream   string `json:"stream"`
	APILevel uint   `json:"api_level"`
}

func (c *SrvReportCmd) downgradeCheckAction(_ *cobra.Command, args []string) error {
	apiLevel, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return err
	}
	c.apiLevel = uint(apiLevel)

	if c.archivePath == "" {
		nc, _, err := prepareHelper("", natsOpts()...)
		if err != nil {
			return err
		}
		c.nc = nc

		if c.waitFor == 0 {
			c.waitFor, err = serverdata.CurrentActiveServers(ctx, nc, opts().Timeout, traceLogger())
			if err != nil {
				return fmt.Errorf("failed to get current active servers: %s", err)
			}
		}
	}

	if !c.json {
		fmt.Println("Obtaining JetStream Asset information")
	}

	src, err := c.dataSource()
	if err != nil {
		return err
	}
	defer src.Close()

	accounts, err := src.CollectAccounts()
	if err != nil {
		return fmt.Errorf("JSZ fetch failed: %v", err)
	}

	var as jsDowngradeAsset
	for _, acc := range accounts {
		for _, s := range acc.Streams {
			streamApiLevel := iu.ParseApiLevel(s.Config.Metadata["_nats.req.level"])
			incompatibleStream := c.apiLevel < streamApiLevel

			if incompatibleStream {
				as.Streams = append(as.Streams, jsDowngradeStreamAsset{
					Account:  acc.Name,
					Name:     s.Name,
					APILevel: streamApiLevel,
				})
			}

			sort.Slice(s.Consumer, func(i, j int) bool {
				return s.Consumer[i].Name < s.Consumer[j].Name
			})

			for _, con := range s.Consumer {
				consumerApiLevel := iu.ParseApiLevel(con.Config.Metadata["_nats.req.level"])
				if c.apiLevel < consumerApiLevel || (c.all && incompatibleStream) {
					displayStream := s.Name
					if c.all && incompatibleStream {
						displayStream += "*"
					}
					as.Consumers = append(as.Consumers, jsDowngradeConsumerAsset{
						Account:  acc.Name,
						Name:     con.Name,
						Stream:   displayStream,
						APILevel: consumerApiLevel,
					})
				}
			}
		}
	}

	c.renderDowngrade(as)
	return nil
}

func (c *SrvReportCmd) renderDowngrade(as jsDowngradeAsset) {
	if !c.json {
		c.printDowngradeReport(as)
		return
	}

	if err := iu.PrintJSON(as); err != nil {
		fmt.Printf("unable to generate json output: %s\n", err)
	}
}

func (c *SrvReportCmd) printDowngradeReport(as jsDowngradeAsset) {
	empty := true
	if len(as.Streams) > 0 {
		empty = false
		streamsTable := iu.NewTableWriterf(opts(), "Assets: Streams")
		streamsTable.AddHeaders("Account", "Stream", "Required API level")
		for _, s := range as.Streams {
			streamsTable.AddRow(s.Account, s.Name, s.APILevel)
		}
		fmt.Println(streamsTable.Render())
	}

	if len(as.Consumers) > 0 {
		empty = false
		consumersTable := iu.NewTableWriterf(opts(), "Assets: Consumers")
		consumersTable.AddHeaders("Account", "Stream", "Consumer", "Required API level")
		for _, c := range as.Consumers {
			consumersTable.AddRow(c.Account, c.Stream, c.Name, c.APILevel)
		}
		fmt.Println(consumersTable.Render())
	}

	if empty {
		fmt.Println("All assets are compatible with the specified API level.")
	}
}
