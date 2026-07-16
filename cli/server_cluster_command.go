// Copyright 2020-2024 The NATS Authors
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
	"slices"
	"strings"
	"time"

	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/jsm.go/connbalancer"
	"github.com/nats-io/jsm.go/serverdata"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/spf13/cobra"
)

type SrvClusterCmd struct {
	json              bool
	force             bool
	peer              string
	placementCluster  string
	placementNode     string
	placementTags     []string
	balanceServerName string
	balanceCluster    string
	balanceIdle       time.Duration
	balanceAccount    string
	balanceSubject    string
	balanceRunTime    time.Duration
	balanceKinds      []string
}

func configureServerClusterCommand(srv *cobra.Command) {
	c := &SrvClusterCmd{}

	cluster := addCommand(srv, "cluster", "Manage JetStream Clustering")
	cluster.Aliases = []string{"r", "raft"}

	balance := addCommand(cluster, "balance", "Balance cluster connections")
	balance.RunE = c.balanceAction
	cmdAddTags(balance, "scope:system", "impact:rw")
	addArgWithDefault(balance, "duration", "Spread balance requests over a certain duration", "2m", "duration")
	balance.Flags().StringVar(&c.balanceServerName, "server-name", "", "Restrict balancing to a specific server")
	flagPlaceholder(balance, "server-name", "NAME")
	balance.Flags().StringVar(&c.balanceCluster, "cluster", "", "Restrict balancing to servers in a specific cluster")
	flagPlaceholder(balance, "cluster", "NAME")
	balance.Flags().DurationVar(&c.balanceIdle, "idle", 0, "Balance connections that has been idle for a period")
	flagPlaceholder(balance, "idle", "DURATION")
	balance.Flags().StringVar(&c.balanceAccount, "account", "", "Balance connections in a certain account only")
	balance.Flags().StringVar(&c.balanceSubject, "subject", "", "Balance connections interested in certain subjects")
	c.balanceKinds = []string{"Client"}
	balance.Flags().Var(newEnumsValue(&c.balanceKinds, "Client", "Leafnode"), "kind", "Balance only certain kinds of connection (*Client, Leafnode)")
	balance.Flags().BoolVarP(&c.force, "force", "f", false, "Force rebalance without prompting")

	sd := addCommand(cluster, "step-down", "Force a new leader election by standing down the current meta leader")
	sd.Aliases = []string{"stepdown", "sd", "elect", "down", "d"}
	sd.RunE = c.metaLeaderStandDownAction
	cmdAddTags(sd, "scope:system", "impact:rw")
	sd.Flags().StringVar(&c.placementCluster, "cluster", "", "Request placement of the leader in a specific cluster")
	sd.Flags().StringArrayVar(&c.placementTags, "tags", nil, "Request placement of the leader on nodes with specific tag(s)")
	sd.Flags().StringVar(&c.placementNode, "host", "", "Request placement of the leader on a specific node")
	sd.Flags().BoolVarP(&c.json, "json", "j", false, "Produce JSON output")
	sd.Flags().BoolVarP(&c.force, "force", "f", false, "Force leader step down ignoring current leader")

	rm := addCommand(cluster, "peer-remove", "Removes a server from a JetStream cluster")
	rm.Aliases = []string{"rm", "pr"}
	rm.RunE = c.metaPeerRemoveAction
	cmdAddTags(rm, "scope:system", "impact:rw")
	addArg(rm, "name", "The Server Name or ID to remove from the JetStream cluster", true, "string")
	rm.Flags().BoolVarP(&c.force, "force", "f", false, "Force removal without prompting")
	rm.Flags().BoolVarP(&c.json, "json", "j", false, "Produce JSON output")
}

func (c *SrvClusterCmd) balanceAction(_ *cobra.Command, args []string) error {
	d, err := parseDuration(args[0])
	if err != nil {
		return err
	}
	c.balanceRunTime = d

	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	var clusters []string
	if c.balanceCluster == "" {
		clusters, err = c.detectClusters(nc)
		if err != nil {
			return err
		}
	}

	if !c.force {
		fmt.Println("Re-balancing will disconnect clients without knowing their current state.")
		fmt.Println()
		fmt.Println("The clients will trigger normal reconnect behavior. This can interrupt in-flight work.")
		fmt.Println()
		if len(clusters) > 1 {
			fmt.Printf("WARNING: Balancing %d clusters, pass --cluster to balance a specific cluster", len(clusters))
			fmt.Println()
		}
		ok, err := askConfirmation("Really re-balance connections", false)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("balance canceled")
		}
	}

	level := api.InfoLevel
	if opts().Trace {
		level = api.TraceLevel
	}

	balancer, err := connbalancer.New(nc, c.balanceRunTime, api.NewDefaultLogger(level), connbalancer.ConnectionSelector{
		ServerName:      c.balanceServerName,
		Cluster:         c.balanceCluster,
		Idle:            c.balanceIdle,
		Account:         c.balanceAccount,
		SubjectInterest: c.balanceSubject,
		Kind:            c.balanceKinds,
	})
	if err != nil {
		return err
	}

	balanced, err := balancer.Balance(ctx)
	if err != nil {
		return err
	}

	fmt.Println()

	fmt.Printf("Balanced %s connections\n", f(balanced))

	return nil
}

func (c *SrvClusterCmd) detectClusters(nc *nats.Conn) ([]string, error) {
	reqFn := func(req any, subj string, waitFor int, nc *nats.Conn) ([][]byte, error) {
		return serverdata.DoReq(ctx, req, subj, waitFor, nc, opts().Timeout, traceLogger())
	}
	ds, err := serverdata.NewLive(nc, reqFn, 0)
	if err != nil {
		return nil, err
	}

	varzResults, err := ds.Varz(server.VarzEventOptions{})
	if err != nil {
		return nil, err
	}

	var clusters []string
	for _, resp := range varzResults {
		if resp.Data != nil && resp.Data.Cluster.Name != "" {
			clusterName := resp.Data.Cluster.Name
			if !slices.Contains(clusters, clusterName) {
				clusters = append(clusters, clusterName)
			}
		}
	}

	return clusters, nil
}

func (c *SrvClusterCmd) metaPeerRemoveAction(_ *cobra.Command, args []string) error {
	c.peer = args[0]

	nc, mgr, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	// a configured domain would make the manager send the removal to a domain prefixed
	// API subject which has no responders on the system account, causing an error the user
	// won't expect. Silently ignoring it could cause us to accidentally remove a peer from a
	// cluster we didn't expect, so we terminate early.
	if opts().Config.JSDomain() != "" {
		return fmt.Errorf("the --js-domain option cannot be used with peer-remove: JetStream domains do not apply to the system account, connect without a domain configured")
	}

	reqFn := func(req any, subj string, waitFor int, nc *nats.Conn) ([][]byte, error) {
		return serverdata.DoReq(ctx, req, subj, waitFor, nc, opts().Timeout, traceLogger())
	}

	expected, err := serverdata.CurrentActiveServers(ctx, nc, opts().Timeout, traceLogger())
	if err != nil {
		return err
	}
	ds, err := serverdata.NewLive(nc, reqFn, expected)
	if err != nil {
		return err
	}

	jszResults, err := ds.Jsz(server.JszEventOptions{})
	if err != nil {
		return err
	}

	found := false
	foundName := ""
	foundID := ""
	state := "offline"

	var leaders []*server.ServerAPIJszResponse
	for _, jr := range jszResults {
		if jr.Data == nil || jr.Data.Meta == nil || jr.Server == nil {
			continue
		}
		if jr.Server.Name != jr.Data.Meta.Leader {
			continue
		}
		leaders = append(leaders, jr)
	}

	if len(leaders) == 0 {
		return fmt.Errorf("did not receive a response from the meta leader, ensure the account used has system privileges and appropriate permissions")
	}

	// multiple meta leaders means we can't be sure of what to do. terminate instead of guessing.
	// TODO(ploubser): This will need to be addressed in the server at a later time
	if len(leaders) > 1 {
		return fmt.Errorf("found %d JetStream meta cluster leaders, unable to determine which cluster to remove the peer from", len(leaders))
	}

	srv := leaders[0]

	for _, r := range srv.Data.Meta.Replicas {
		if r.Name == c.peer || r.Peer == c.peer {
			if !r.Offline {
				state = "online"
			}
			foundID = r.Peer
			foundName = r.Name
			found = true
		}
	}

	if !found {
		return fmt.Errorf("did not find a replica named %s", c.peer)
	}

	if !c.force {
		fmt.Println(`Removing peers is intended for peers that will not come back, generally for peers that will remain
in the cluster removing the peer is not needed - simply do your maintenance with the peer shut down.

To return a removed peer to the cluster you have to restart the entire cluster or super-cluster first.
If the peer was online when it was removed you need to restart the removed peer last.

R1 streams on this peer will migrate to other peers but data will be lost, even if the
peer returns later.

R1 consumers on this peer will lose their state and revert to starting configuring 
which may lead to duplicate deliveries.`)

		fmt.Println()

		var remove bool
		if c.peer == foundName || strings.Contains(foundName, foundID) {
			remove, err = askConfirmation(fmt.Sprintf("Really remove %s peer %s", state, foundName), false)
		} else {
			remove, err = askConfirmation(fmt.Sprintf("Really remove %s peer %s with id %s", state, foundName, foundID), false)
		}
		fatalIfError(err, "Could not prompt for confirmation")
		if !remove {
			fmt.Println("Removal canceled")
			os.Exit(0)
		}
	}

	if foundID != "" {
		err = mgr.MetaPeerRemove("", foundID)
	} else {
		err = mgr.MetaPeerRemove(foundName, foundID)
	}
	fatalIfError(err, "Could not remove %s", foundID)

	return nil
}

func (c *SrvClusterCmd) metaLeaderStandDownAction(_ *cobra.Command, _ []string) error {
	nc, mgr, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	// a configured domain would make the manager send the request to a domain prefixed
	// API subject which has no responders on the system account, causing an error the user
	// won't expect. Silently ignoring it could cause us to accidentally step down a leader in
	// a cluster we didn't expect, so we terminate early.
	if opts().Config.JSDomain() != "" {
		return fmt.Errorf("the --js-domain option cannot be used with step-down: JetStream domains do not apply to the system account, connect without a domain configured")
	}

	reqFn := func(req any, subj string, waitFor int, nc *nats.Conn) ([][]byte, error) {
		return serverdata.DoReq(ctx, req, subj, waitFor, nc, opts().Timeout, traceLogger())
	}

	expected, err := serverdata.CurrentActiveServers(ctx, nc, opts().Timeout, traceLogger())
	if err != nil {
		return err
	}
	ds, err := serverdata.NewLive(nc, reqFn, expected)
	if err != nil {
		return err
	}

	getJSI := func() (*server.JSInfo, error) {
		jszResults, err := ds.Jsz(server.JszEventOptions{})
		if err != nil {
			return nil, err
		}

		var leaders []*server.ServerAPIJszResponse
		for _, jr := range jszResults {
			if jr.Data == nil || jr.Data.Meta == nil || jr.Server == nil {
				continue
			}
			if jr.Server.Name != jr.Data.Meta.Leader {
				continue
			}
			leaders = append(leaders, jr)
		}

		if len(leaders) == 0 {
			return nil, fmt.Errorf("did not receive a response from the meta leader, ensure the account used has system privileges and appropriate permissions")
		}

		// multiple meta leaders means we can't be sure of what to do. terminate instead of guessing.
		// TODO(ploubser): This will need to be addressed in the server at a later time
		if len(leaders) > 1 {
			return nil, fmt.Errorf("found %d JetStream meta cluster leaders, unable to determine which cluster to step down", len(leaders))
		}

		return leaders[0].Data, nil
	}

	resp, err := getJSI()
	if err != nil {
		return err
	}

	leader := resp.Meta.Leader
	if leader == "" && !c.force {
		return fmt.Errorf("cluster has no current leader")
	} else if leader == "" {
		leader = "<unknown>"
	}

	if c.placementNode != "" || len(c.placementTags) > 0 {
		// TODO: check api level instead but requires https://github.com/nats-io/nats-server/issues/6681
		fmt.Println("WARNING: Using placement tags or node name required NATS Server 2.11 or newer")
		fmt.Println()
	}

	log.Printf("Requesting leader step down of %q in a %d peer RAFT group", leader, len(resp.Meta.Replicas)+1)
	if c.placementCluster != "" || len(c.placementTags) > 0 || c.placementNode != "" {
		err = mgr.MetaLeaderStandDown(&api.Placement{
			Cluster:   c.placementCluster,
			Tags:      c.placementTags,
			Preferred: c.placementNode,
		})
	} else {
		err = mgr.MetaLeaderStandDown(nil)
	}
	if err != nil {
		return err
	}

	ctr := 0
	start := time.Now()
	for range time.NewTicker(500 * time.Millisecond).C {
		if ctr == 5 {
			return fmt.Errorf("stream did not elect a new leader in time")
		}
		ctr++

		resp, err = getJSI()
		if err != nil {
			log.Printf("Failed to retrieve Cluster State: %s", err)
			continue
		}

		if resp.Meta.Leader != leader {
			log.Printf("New leader elected %q", resp.Meta.Leader)
			os.Exit(0)
		}
	}

	if resp.Meta.Leader == leader {
		log.Printf("Leader did not change after %s", time.Since(start).Round(time.Millisecond))
		os.Exit(1)
	}

	return nil
}
