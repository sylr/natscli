// Copyright 2023-2026 The NATS Authors
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
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/nats-io/jsm.go/serverdata"
	au "github.com/nats-io/natscli/internal/auth"
	iu "github.com/nats-io/natscli/internal/util"
	"gopkg.in/yaml.v3"

	"github.com/AlecAivazis/survey/v2"
	"github.com/dustin/go-humanize"
	"github.com/fatih/color"
	"github.com/nats-io/nats-server/v2/server"
	ab "github.com/synadia-io/jwt-auth-builder.go"

	"github.com/spf13/cobra"
)

type authAccountCommand struct {
	accountName             string
	advertise               bool
	advertiseIsSet          bool
	bearerAllowed           bool
	bearerAllowedIsSet      bool
	connTypes               []string
	defaults                bool
	description             string
	descriptionIsSet        bool
	expiry                  time.Duration
	exportName              string
	force                   bool
	json                    bool
	isService               bool
	jetStream               bool
	jetStreamIsSet          bool
	listNames               bool
	locale                  string
	maxAckPending           int64
	maxAckPendingIsSet      bool
	maxConns                int64
	maxConnsIsSet           bool
	maxConsumers            int64
	maxConsumersIsSet       bool
	maxExports              int64
	maxExportsIsSet         bool
	maxImports              int64
	maxImportsIsSet         bool
	maxLeafnodes            int64
	maxLeafNodesIsSet       bool
	maxPayload              int64
	maxPayloadString        string
	maxStreams              int64
	maxStreamsIsSet         bool
	maxSubs                 int64
	maxSubIsSet             bool
	memMax                  int64
	memMaxStream            int64
	memMaxStreamString      string
	memMaxString            string
	operatorName            string
	output                  string
	pubAllow                []string
	pubDeny                 []string
	showJWT                 bool
	skRole                  string
	storeMax                int64
	storeMaxStream          int64
	storeMaxStreamString    string
	storeMaxString          string
	streamSizeRequired      bool
	streamSizeRequiredIsSet bool
	subAllow                []string
	subDeny                 []string
	subject                 string
	tokenPosition           uint
	url                     *url.URL
	importName              string
	localSubject            string
	share                   bool
	shareIsSet              bool
	allowTrace              bool
	allowTraceIsSet         bool
	importAccount           string
	bucketName              string
	prefix                  string
	tags                    []string
	rmTags                  []string
	signingKey              string
	mapSource               string
	mapTarget               string
	mapWeight               uint
	mapCluster              string
	inputFile               string
	clusterTraffic          string
	clusterTrafficIsSet     bool
}

func configureAuthAccountCommand(auth commandHost) {
	c := &authAccountCommand{}

	acct := addCommand(auth, "account", "Manage NATS Accounts")
	acct.Aliases = []string{"a", "acct", "act"}
	addCreateFlags := func(f *cobra.Command, edit bool) {
		negatableBoolVar(f, &c.bearerAllowed, "bearer", false, "Allows bearer tokens")
		f.Flags().Int64Var(&c.maxConns, "connections", -1, "Maximum allowed connections")
		f.Flags().DurationVar(&c.expiry, "expiry", 0, "How long this account should be valid for as a duration")
		flagPlaceholder(f, "expiry", "DURATION")
		f.Flags().Int64Var(&c.maxExports, "exports", -1, "Maximum allowed exports")
		f.Flags().Int64Var(&c.maxImports, "imports", -1, "Maximum allowed imports")
		negatableBoolVar(f, &c.jetStream, "jetstream", false, "Enables JetStream")
		f.Flags().Int64Var(&c.maxConsumers, "js-consumers", -1, "Sets the maximum Consumers any Stream in the account can have")
		f.Flags().StringVar(&c.storeMaxString, "js-disk", "", "Sets a Disk Storage quota")
		flagPlaceholder(f, "js-disk", "BYTES")
		f.Flags().StringVar(&c.storeMaxStreamString, "js-disk-stream", "-1", "Sets the maximum size a Disk Storage stream may be")
		flagPlaceholder(f, "js-disk-stream", "BYTES")
		f.Flags().Int64Var(&c.maxAckPending, "js-max-pending", 0, "Default Max Ack Pending for Tier 0 limits")
		flagPlaceholder(f, "js-max-pending", "MESSAGES")
		f.Flags().StringVar(&c.memMaxString, "js-memory", "", "Sets a Memory Storage quota")
		flagPlaceholder(f, "js-memory", "BYTES")
		f.Flags().StringVar(&c.memMaxStreamString, "js-memory-stream", "-1", "Sets the maximum size a Memory Storage stream may be")
		flagPlaceholder(f, "js-memory-stream", "BYTES")
		f.Flags().BoolVar(&c.streamSizeRequired, "js-stream-size-required", false, "Requires Streams to have a maximum size declared")
		f.Flags().Int64Var(&c.maxStreams, "js-streams", -1, "Sets the maximum Streams the account can have")
		f.Flags().Var(newEnumValue(&c.clusterTraffic, "", "owner", "system"), "js-cluster-traffic", "Sets the account used for JetStream cluster traffic")
		flagPlaceholder(f, "js-cluster-traffic", "ACCOUNT")
		f.Flags().Int64Var(&c.maxLeafnodes, "leafnodes", -1, "Maximum allowed Leafnode connections")
		f.Flags().StringVar(&c.maxPayloadString, "payload", "-1", "Maximum allowed payload")
		flagPlaceholder(f, "payload", "BYTES")
		f.Flags().Int64Var(&c.maxSubs, "subscriptions", -1, "Maximum allowed subscriptions")
		f.Flags().StringArrayVar(&c.tags, "tags", nil, "Tags to assign to this Account")
		if edit {
			f.Flags().StringArrayVar(&c.rmTags, "no-tags", nil, "Tags to remove from this Account")
		}
	}

	add := addCommand(acct, "add", "Adds a new Account")
	add.Aliases = []string{"create", "new"}
	add.RunE = c.addAction
	cmdAddTags(add, "scope:system", "impact:rw")
	addArg(add, "account", "Unique name for this Account", false, "string")
	add.Flags().StringVar(&c.operatorName, "operator", "", "Operator to add the account to")
	add.Flags().StringVar(&c.signingKey, "key", "", "The public key to use when signing the user")
	addCreateFlags(add, false)
	add.Flags().BoolVar(&c.defaults, "defaults", false, "Accept default values without prompting")

	info := addCommand(acct, "info", "Show Account information")
	info.Aliases = []string{"i", "show", "view"}
	info.RunE = c.infoAction
	cmdAddTags(info, "scope:system", "impact:ro")
	addArg(info, "account", "Account to view", false, "string")
	info.Flags().StringVar(&c.operatorName, "operator", "", "Operator hosting the account")
	info.Flags().BoolVarP(&c.json, "json", "j", false, "Produce JSON output")

	edit := addCommand(acct, "edit", "Edit Account settings")
	edit.Aliases = []string{"update"}
	edit.RunE = c.editAction
	cmdAddTags(edit, "scope:system", "impact:rw")
	addArg(edit, "account", "Unique name for this Account", false, "string")
	edit.Flags().StringVar(&c.operatorName, "operator", "", "Operator to add the account to")
	addCreateFlags(edit, false)

	ls := addCommand(acct, "ls", "List Accounts")
	ls.RunE = c.lsAction
	cmdAddTags(ls, "scope:system", "impact:ro")
	addArg(ls, "operator", "Operator to act on", false, "string")
	ls.Flags().BoolVar(&c.listNames, "names", false, "Show just the Account names")

	//rm := acct.Command("rm", "Removes an Account").Action(c.rmAction)
	//rm.Arg("name", "Account to view").StringVar(&c.accountName)
	//rm.Flag("operator", "Operator hosting the Account").StringVar(&c.operatorName)
	//rm.Flag("force", "Removes without prompting").Short('f').UnNegatableBoolVar(&c.force)

	push := addCommand(acct, "push", "Push the Account to the NATS Resolver")
	push.RunE = c.pushAction
	cmdAddTags(push, "scope:system", "impact:rw")
	addArg(push, "account", "Account to act on", false, "string")
	push.Flags().StringVar(&c.operatorName, "operator", "", "Operator to act on")
	push.Flags().BoolVar(&c.showJWT, "show", false, "Show the Account JWT before pushing")

	query := addCommand(acct, "query", "Pull the Account from the NATS Resolver and view it")
	query.Aliases = []string{"pull"}
	query.RunE = c.queryAction
	cmdAddTags(query, "scope:system", "impact:ro")
	addArg(query, "account", "Account to act on", true, "string")
	addArg(query, "output", "Saves the JWT to a file", false, "string")
	query.Flags().StringVar(&c.operatorName, "operator", "", "Operator to act on")

	imports := addCommand(acct, "imports", "Manage account Imports")
	imports.Aliases = []string{"i", "imp", "import"}

	impAdd := addCommand(imports, "add", "Adds an Import")
	impAdd.Aliases = []string{"new", "a", "n"}
	impAdd.RunE = c.importAddAction
	cmdAddTags(impAdd, "scope:system", "impact:rw")
	addArg(impAdd, "name", "A unique name for the import", true, "string")
	addArg(impAdd, "subject", "The Subject to import", true, "string")
	addArg(impAdd, "account", "Account to import into", false, "string")
	impAdd.Flags().StringVar(&c.importAccount, "source", "", "The account public key to import from")
	impAdd.Flags().StringVar(&c.localSubject, "local", "", "The local Subject to use for the import")
	impAdd.Flags().BoolVar(&c.share, "share", false, "Shares connection information with the exporter")
	impAdd.Flags().BoolVar(&c.allowTrace, "traceable", false, "Enable tracing messages across Stream imports")
	impAdd.Flags().BoolVar(&c.isService, "service", false, "Sets the import to be a Service rather than a Stream")
	impAdd.Flags().StringVar(&c.operatorName, "operator", "", "Operator hosting the account")

	impInfo := addCommand(imports, "info", "Show information for an Import")
	impInfo.Aliases = []string{"i", "show", "view"}
	impInfo.RunE = c.importInfoAction
	cmdAddTags(impInfo, "scope:system", "impact:ro")
	addArg(impInfo, "subject", "Export to view by subject", false, "string")
	addArg(impInfo, "account", "Account to act on", false, "string")
	impInfo.Flags().StringVar(&c.operatorName, "operator", "", "Operator hosting the account")
	impInfo.Flags().BoolVarP(&c.json, "json", "j", false, "Produce JSON output")

	impEdit := addCommand(imports, "edit", "Edits an Import")
	impEdit.Aliases = []string{"update"}
	impEdit.RunE = c.importEditAction
	cmdAddTags(impEdit, "scope:system", "impact:rw")
	addArg(impEdit, "subject", "The Local import Subject to edit", true, "string")
	addArg(impEdit, "account", "Account to act on", false, "string")
	impEdit.Flags().StringVar(&c.localSubject, "local", "", "The local Subject to use for the import")
	impEdit.Flags().BoolVar(&c.share, "share", false, "Shares connection information with the exporter")
	impEdit.Flags().BoolVar(&c.allowTrace, "traceable", false, "Enable tracing messages across Stream imports")
	impEdit.Flags().StringVar(&c.operatorName, "operator", "", "Operator hosting the account")

	impLs := addCommand(imports, "ls", "List Imports")
	impLs.Aliases = []string{"list"}
	impLs.RunE = c.importLsAction
	cmdAddTags(impLs, "scope:system", "impact:ro")
	addArg(impLs, "account", "Account to act on", false, "string")
	addArg(impLs, "operator", "Operator to act on", false, "string")

	impRm := addCommand(imports, "rm", "Removes an Import")
	impRm.RunE = c.importRmAction
	cmdAddTags(impRm, "scope:system", "impact:rw")
	addArg(impRm, "subject", "Import to remove by local subject", true, "string")
	addArg(impRm, "account", "Account to act on", false, "string")
	impRm.Flags().StringVar(&c.operatorName, "operator", "", "Operator hosting the account")
	impRm.Flags().BoolVarP(&c.force, "force", "f", false, "Removes without prompting")

	impKv := addCommand(imports, "kv", "Imports a KV bucket")
	impKv.Hidden = true
	impKv.RunE = c.importKvAction
	cmdAddTags(impKv, "scope:system", "impact:rw")
	addArg(impKv, "bucket", "The bucket to export", true, "string")
	addArg(impKv, "prefix", "The prefix to mount the bucket on", true, "string")
	addArg(impKv, "source", "The account public key to import from", true, "string")

	exports := addCommand(acct, "exports", "Manage account Exports")
	exports.Aliases = []string{"e", "exp", "export"}

	expAdd := addCommand(exports, "add", "Adds an Export")
	expAdd.Aliases = []string{"new", "a", "n"}
	expAdd.RunE = c.exportAddAction
	cmdAddTags(expAdd, "scope:system", "impact:rw")
	addArg(expAdd, "name", "A unique name for the Export", true, "string")
	addArg(expAdd, "subject", "The Subject to export", true, "string")
	addArg(expAdd, "account", "Account to act on", false, "string")
	expAdd.Flags().StringVar(&c.operatorName, "operator", "", "Operator hosting the account")
	expAdd.Flags().StringVar(&c.description, "description", "", "Friendly description")
	expAdd.Flags().Var(newURLValue(&c.url), "url", "Sets a URL for further information")
	expAdd.Flags().UintVar(&c.tokenPosition, "token-position", 0, "The position to use for the Account name")
	expAdd.Flags().BoolVar(&c.advertise, "advertise", false, "Advertise the Export")
	expAdd.Flags().BoolVar(&c.isService, "service", false, "Sets the Export to be a Service rather than a Stream")

	expInfo := addCommand(exports, "info", "Show information for an Export")
	expInfo.Aliases = []string{"i", "show", "view"}
	expInfo.RunE = c.exportInfoAction
	cmdAddTags(expInfo, "scope:system", "impact:ro")
	addArg(expInfo, "subject", "Export to view by subject", false, "string")
	addArg(expInfo, "account", "Account to act on", false, "string")
	expInfo.Flags().StringVar(&c.operatorName, "operator", "", "Operator hosting the account")
	expInfo.Flags().BoolVarP(&c.json, "json", "j", false, "Produce JSON output")

	expEdit := addCommand(exports, "edit", "Edits an Export")
	expEdit.Aliases = []string{"update"}
	expEdit.RunE = c.exportEditAction
	cmdAddTags(expEdit, "scope:system", "impact:rw")
	addArg(expEdit, "subject", "The Export Subject to edit", true, "string")
	addArg(expEdit, "account", "Account to act on", false, "string")
	expEdit.Flags().StringVar(&c.operatorName, "operator", "", "Operator hosting the account")
	expEdit.Flags().StringVar(&c.description, "description", "", "Friendly description")
	expEdit.Flags().Var(newURLValue(&c.url), "url", "Sets a URL for further information")
	expEdit.Flags().UintVar(&c.tokenPosition, "token-position", 0, "The position to use for the Account name")
	negatableBoolVar(expEdit, &c.advertise, "advertise", false, "Advertise the Export")

	expLs := addCommand(exports, "ls", "List Exports")
	expLs.Aliases = []string{"list"}
	expLs.RunE = c.exportLsAction
	cmdAddTags(expLs, "scope:system", "impact:ro")
	addArg(expLs, "account", "Account to act on", false, "string")
	expLs.Flags().StringVar(&c.operatorName, "operator", "", "Operator to act on")

	expRm := addCommand(exports, "rm", "Removes an Export")
	expRm.RunE = c.exportRmAction
	cmdAddTags(expRm, "scope:system", "impact:rw")
	addArg(expRm, "subject", "Export to remove by subject", true, "string")
	addArg(expRm, "account", "Account to act on", false, "string")
	expRm.Flags().StringVar(&c.operatorName, "operator", "", "Operator hosting the account")
	expRm.Flags().BoolVarP(&c.force, "force", "f", false, "Removes without prompting")

	expKv := addCommand(exports, "kv", "Exports a KV bucket")
	expKv.Hidden = true
	expKv.RunE = c.exportKvAction
	cmdAddTags(expKv, "scope:system", "impact:rw")
	addArg(expKv, "bucket", "The bucket to export", true, "string")

	sk := addCommand(acct, "keys", "Manage Scoped Signing Keys")
	sk.Aliases = []string{"sk", "s"}

	skadd := addCommand(sk, "add", "Adds a signing key")
	skadd.Aliases = []string{"new", "a", "n"}
	skadd.RunE = c.skAddAction
	cmdAddTags(skadd, "scope:system", "impact:rw")
	addArg(skadd, "account", "Account to act on", false, "string")
	addArg(skadd, "role", "The role to add a key for", false, "string")
	skadd.Flags().StringVar(&c.operatorName, "operator", "", "Operator to act on")
	skadd.Flags().StringVar(&c.description, "description", "", "Description for the signing key")
	skadd.Flags().Int64Var(&c.maxSubs, "subscriptions", -1, "Maximum allowed subscriptions")
	skadd.Flags().StringVar(&c.maxPayloadString, "payload", "", "Maximum allowed payload")
	flagPlaceholder(skadd, "payload", "BYTES")
	negatableBoolVar(skadd, &c.bearerAllowed, "bearer", false, "Allows bearer tokens")
	skadd.Flags().StringVar(&c.locale, "locale", "", "Locale for the client")
	skadd.Flags().Var(newEnumsValue(&c.connTypes, "nats", "ws", "leaf", "wsleaf", "mqtt"), "connection", "Set the allowed connections (nats, ws, wsleaf, mqtt)")
	skadd.Flags().StringArrayVar(&c.pubAllow, "pub-allow", nil, "Sets subjects where publishing is allowed")
	skadd.Flags().StringArrayVar(&c.pubDeny, "pub-deny", nil, "Sets subjects where publishing is allowed")
	skadd.Flags().StringArrayVar(&c.subAllow, "sub-allow", nil, "Sets subjects where subscribing is allowed")
	skadd.Flags().StringArrayVar(&c.subDeny, "sub-deny", nil, "Sets subjects where subscribing is allowed")

	skInfo := addCommand(sk, "info", "Show information for a Scoped Signing Key")
	skInfo.Aliases = []string{"i", "show", "view"}
	skInfo.RunE = c.skInfoAction
	cmdAddTags(skInfo, "scope:system", "impact:ro")
	addArg(skInfo, "account", "Account to view", false, "string")
	addArg(skInfo, "key", "The role or key to view", false, "string")
	skInfo.Flags().StringVar(&c.operatorName, "operator", "", "Operator to act on")
	skInfo.Flags().BoolVarP(&c.json, "json", "j", false, "Produce JSON output")

	skls := addCommand(sk, "ls", "List Scoped Signing Keys")
	skls.Aliases = []string{"list"}
	skls.RunE = c.skListAction
	cmdAddTags(skls, "scope:system", "impact:ro")
	addArg(skls, "account", "Account to act on", false, "string")
	skls.Flags().StringVar(&c.operatorName, "operator", "", "Operator to act on")

	skrm := addCommand(sk, "rm", "Remove a scoped signing key")
	skrm.RunE = c.skRmAction
	cmdAddTags(skrm, "scope:system", "impact:rw")
	addArg(skrm, "account", "Account to act on", false, "string")
	skrm.Flags().StringVar(&c.skRole, "key", "", "The key to remove")
	skrm.Flags().StringVar(&c.operatorName, "operator", "", "Operator to act on")
	skrm.Flags().BoolVarP(&c.force, "force", "f", false, "Removes without prompting")

	mappings := addCommand(acct, "mappings", "Manage account level subject mapping and partitioning")
	mappings.Aliases = []string{"m", "mapping", "map"}

	mappingsaAdd := addCommand(mappings, "add", "Add a new mapping")
	mappingsaAdd.Aliases = []string{"new", "a"}
	mappingsaAdd.RunE = c.mappingAddAction
	cmdAddTags(mappingsaAdd, "scope:system", "impact:rw")
	addArg(mappingsaAdd, "account", "Account to create the mappings on", false, "string")
	addArg(mappingsaAdd, "source", "The source subject of the mapping", false, "string")
	addArg(mappingsaAdd, "target", "The target subject of the mapping", false, "string")
	addArgWithDefault(mappingsaAdd, "weight", "The weight (%) of the mapping", "100", "uint")
	addArg(mappingsaAdd, "cluster", "Limit the mappings to a specific cluster", false, "string")
	mappingsaAdd.Flags().StringVar(&c.operatorName, "operator", "", "Operator to act on")
	mappingsaAdd.Flags().Var(newExistingFileValue(&c.inputFile), "config", "json or yaml file to read configuration from")

	mappingsls := addCommand(mappings, "ls", "List mappings")
	mappingsls.Aliases = []string{"list"}
	mappingsls.RunE = c.mappingListAction
	cmdAddTags(mappingsls, "scope:system", "impact:ro")
	addArg(mappingsls, "account", "Account to list the mappings from", false, "string")
	mappingsls.Flags().StringVar(&c.operatorName, "operator", "", "Operator to act on")

	mappingsrm := addCommand(mappings, "rm", "Remove a mapping")
	mappingsrm.RunE = c.mappingRmAction
	cmdAddTags(mappingsrm, "scope:system", "impact:rw")
	addArg(mappingsrm, "account", "Account to remove the mappings from", false, "string")
	addArg(mappingsrm, "source", "The source subject of the mapping", false, "string")
	mappingsrm.Flags().StringVar(&c.operatorName, "operator", "", "Operator to act on")

	mappingsinfo := addCommand(mappings, "info", "Show information about a mapping")
	mappingsinfo.Aliases = []string{"i", "show", "view"}
	mappingsinfo.RunE = c.mappingInfoAction
	cmdAddTags(mappingsinfo, "scope:system", "impact:ro")
	addArg(mappingsinfo, "account", "Account to inspect the mappings from", false, "string")
	addArg(mappingsinfo, "source", "The source subject of the mapping", false, "string")
	mappingsinfo.Flags().StringVar(&c.operatorName, "operator", "", "Operator to act on")
	mappingsinfo.Flags().BoolVarP(&c.json, "json", "j", false, "Produce JSON output")
}

func (c *authAccountCommand) selectAccount(pick bool) (*ab.AuthImpl, ab.Operator, ab.Account, error) {
	auth, oper, acct, err := au.SelectOperatorAccount(c.operatorName, c.accountName, pick)
	if err != nil {
		return nil, nil, nil, err
	}

	c.operatorName = oper.Name()
	c.accountName = acct.Name()

	return auth, oper, acct, nil
}

func (c *authAccountCommand) selectOperator(pick bool) (*ab.AuthImpl, ab.Operator, error) {
	auth, oper, err := au.SelectOperator(c.operatorName, pick, true)
	if err != nil {
		return nil, nil, err
	}

	c.operatorName = oper.Name()

	return auth, oper, err
}

func (c *authAccountCommand) queryAction(_ *cobra.Command, args []string) error {
	c.accountName = args[0]
	c.output = argValue(args, 1)

	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	_, oper, err := au.SelectOperator(c.operatorName, true, true)
	if err != nil {
		return err
	}

	acct, err := au.SelectAccount(oper, c.accountName, "")
	if err != nil {
		return err
	}

	var token string
	err = serverdata.DoReqAsync(ctx, nil, fmt.Sprintf("$SYS.REQ.ACCOUNT.%s.CLAIMS.LOOKUP", acct.Subject()), 1, nc, opts().Timeout, traceLogger(), func(b []byte) {
		token = string(b)
	})
	if err != nil {
		return err
	}

	if token == "" {
		return fmt.Errorf("did not receive a valid token from the server")
	}

	if c.output != "" {
		err = os.WriteFile(c.output, []byte(token), 0600)
		if err != nil {
			return err
		}
	}

	acct, err = ab.NewAccountFromJWT(token)
	if err != nil {
		return err
	}

	return c.fShowAccount(os.Stdout, nil, acct)
}

func (c *authAccountCommand) pushAction(_ *cobra.Command, args []string) error {
	c.accountName = argValue(args, 0)

	_, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	if c.showJWT {
		fmt.Printf("Account JWT for %s\n", c.accountName)
		fmt.Println()
		fmt.Println(acct.JWT())
		fmt.Println()
	}

	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	expect, _ := serverdata.CurrentActiveServers(ctx, nc, opts().Timeout, traceLogger())
	if expect > 0 {
		fmt.Printf("Updating account %s (%s) on %d server(s)\n", acct.Name(), acct.Subject(), expect)
	} else {
		fmt.Printf("Updating Account %s (%s) on all servers\n", acct.Name(), acct.Subject())
	}
	fmt.Println()

	errStr := color.RedString("X")
	okStr := color.GreenString("✓")
	updated := 0
	failed := 0

	subj := fmt.Sprintf("$SYS.REQ.ACCOUNT.%s.CLAIMS.UPDATE", acct.Subject())
	err = serverdata.DoReqAsync(ctx, acct.JWT(), subj, expect, nc, opts().Timeout, traceLogger(), func(msg []byte) {
		update := server.ServerAPIClaimUpdateResponse{}
		err = json.Unmarshal(msg, &update)
		if err != nil {
			fmt.Printf("%s Invalid JSON response received: %v: %s\n", errStr, err, string(msg))
			failed++
			return
		}

		if update.Error != nil {
			fmt.Printf("%s Update failed on %s: %v\n", errStr, update.Server.Name, update.Error.Description)
			failed++
			return
		}

		fmt.Printf("%s Update completed on %s\n", okStr, update.Server.Name)
		updated++
	})
	if err != nil {
		return err
	}

	fmt.Println()
	if expect > 0 {
		fmt.Printf("Success %d Failed %d Expected %d\n", updated, failed, expect)
	} else {
		fmt.Printf("Success %d Failed %d\n", updated, failed)
	}

	if failed > 0 {
		if expect > 0 {
			return fmt.Errorf("update failed on %d/%d servers", failed, expect)
		}
		return fmt.Errorf("update failed on %d servers", failed)
	}

	if updated == 0 {
		return fmt.Errorf("no servers were updated")
	}

	if expect > 0 && (updated+failed != expect) {
		return fmt.Errorf("received updated from only %d out of %d servers", updated+failed, expect)
	}

	return nil
}

func (c *authAccountCommand) skRmAction(_ *cobra.Command, args []string) error {
	c.accountName = argValue(args, 0)

	auth, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	sk, err := au.SelectSigningKey(acct, c.skRole)
	if err != nil {
		return err
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really remove the Scoped Signing Key %s with role %s", sk.Key(), sk.Role()), false)
		if err != nil {
			return err
		}

		if !ok {
			return nil
		}
	}

	ok, err := acct.ScopedSigningKeys().Delete(sk.Key())
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("key %q not found", sk.Key())
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	fmt.Printf("key %q removed\n", sk.Key())

	return nil
}

func (c *authAccountCommand) skInfoAction(_ *cobra.Command, args []string) error {
	c.accountName = argValue(args, 0)
	c.skRole = argValue(args, 1)

	_, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	sk, err := au.SelectSigningKey(acct, c.skRole)
	if err != nil {
		return err
	}

	out, err := c.showSk(sk)
	if err != nil {
		return err
	}

	fmt.Println(out)
	return nil
}

func (c *authAccountCommand) skAddAction(_ *cobra.Command, args []string) error {
	c.accountName = argValue(args, 0)
	c.skRole = argValue(args, 1)

	auth, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	if c.skRole == "" {
		err := iu.AskOne(&survey.Input{
			Message: "Role Name",
			Help:    "The role to associate with this key",
		}, &c.skRole, survey.WithValidator(survey.Required))
		if err != nil {
			return err
		}
	}

	if c.maxPayloadString != "" {
		c.maxPayload, err = iu.ParseStringAsBytes(c.maxPayloadString, 64)
		if err != nil {
			return err
		}
	}

	scope, err := acct.ScopedSigningKeys().AddScope(c.skRole)
	if err != nil {
		return err
	}

	if c.description != "" {
		err = scope.SetDescription(c.description)
		if err != nil {
			return err
		}
	}

	limits := scope.(au.UserLimitsManager).UserPermissionLimits()
	limits.Subs = c.maxSubs
	limits.Payload = c.maxPayload
	limits.BearerToken = c.bearerAllowed
	limits.Locale = c.locale
	limits.Pub.Allow = c.pubAllow
	limits.Pub.Deny = c.pubDeny
	limits.Sub.Allow = c.subAllow
	limits.Sub.Deny = c.subDeny
	if len(c.connTypes) > 0 {
		limits.AllowedConnectionTypes = c.connectionTypes()
	}

	err = scope.(au.UserLimitsManager).SetUserPermissionLimits(limits)
	if err != nil {
		return err
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	return c.fShowSk(os.Stdout, scope)
}

func (c *authAccountCommand) fShowSk(w io.Writer, limits ab.ScopeLimits) error {
	out, err := c.showSk(limits)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(w, out)

	return err
}

func (c *authAccountCommand) showSk(limits ab.ScopeLimits) (string, error) {
	if c.json {
		return iu.ToJSON(limits)
	}

	cols := newColumnsf("Scoped Signing Key %s", limits.Key())

	cols.AddSectionTitle("Config")

	cols.AddRowIfNotEmpty("Description", limits.Description())
	cols.AddRow("Key", limits.Key())
	cols.AddRow("Role", limits.Role())

	err := au.RenderUserLimits(limits, cols)
	if err != nil {
		return "", err
	}

	return cols.Render()
}

func (c *authAccountCommand) connectionTypes() []string {
	var types []string

	for _, t := range c.connTypes {
		switch t {
		case "nats":
			types = append(types, "STANDARD")
		case "ws":
			types = append(types, "WEBSOCKET")
		case "wsleaf":
			types = append(types, "LEAFNODE_WS")
		case "leaf":
			types = append(types, "LEAFNODE")
		case "mqtt":
			types = append(types, "MQTT")
		}
	}

	return types
}

func (c *authAccountCommand) skListAction(_ *cobra.Command, args []string) error {
	c.accountName = argValue(args, 0)

	_, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	var table *iu.Table

	if len(acct.ScopedSigningKeys().List()) > 0 {
		table = iu.NewTableWriterf(opts(), "Scoped Signing Keys")
		table.AddHeaders("Role", "Key", "Description", "Max Subscriptions", "Pub Perms", "Sub Perms")
		for _, sk := range acct.ScopedSigningKeys().List() {
			scope, _ := acct.ScopedSigningKeys().GetScope(sk)

			pubs := len(scope.PubPermissions().Allow()) + len(scope.PubPermissions().Deny())
			subs := len(scope.SubPermissions().Allow()) + len(scope.SubPermissions().Deny())

			table.AddRow(scope.Role(), scope.Key(), scope.Description(), scope.MaxSubscriptions(), pubs, subs)
		}
		fmt.Println(table.Render())
		fmt.Println()
	}

	if table == nil {
		fmt.Println("No Scoped Signing Keys or Roles defined")
	}

	return nil
}

func (c *authAccountCommand) editAction(cmd *cobra.Command, args []string) error {
	c.accountName = argValue(args, 0)
	c.bearerAllowedIsSet = cmd.Flags().Changed("bearer")
	c.maxConnsIsSet = cmd.Flags().Changed("connections")
	c.maxExportsIsSet = cmd.Flags().Changed("exports")
	c.maxImportsIsSet = cmd.Flags().Changed("imports")
	c.jetStreamIsSet = cmd.Flags().Changed("jetstream")
	c.maxConsumersIsSet = cmd.Flags().Changed("js-consumers")
	c.maxAckPendingIsSet = cmd.Flags().Changed("js-max-pending")
	c.streamSizeRequiredIsSet = cmd.Flags().Changed("js-stream-size-required")
	c.maxStreamsIsSet = cmd.Flags().Changed("js-streams")
	c.clusterTrafficIsSet = cmd.Flags().Changed("js-cluster-traffic")
	c.maxLeafNodesIsSet = cmd.Flags().Changed("leafnodes")
	c.maxSubIsSet = cmd.Flags().Changed("subscriptions")

	auth, operator, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	jsEnabled := acct.Limits().JetStream().IsJetStreamEnabled()
	limits := acct.Limits().(au.OperatorLimitsManager).OperatorLimits()
	// copy existing settings into the flag settings so parsing treats those as defaults unless users set values
	if c.maxPayloadString == "" {
		c.maxPayloadString = strconv.Itoa(int(limits.Payload))
	}
	if !c.maxConnsIsSet {
		c.maxConns = limits.Conn
	}
	if !c.maxSubIsSet {
		c.maxSubs = limits.Subs
	}
	if !c.maxLeafNodesIsSet {
		c.maxLeafnodes = limits.LeafNodeConn
	}
	if !c.maxExportsIsSet {
		c.maxExports = limits.Exports
	}
	if !c.maxImportsIsSet {
		c.maxImports = limits.Imports
	}
	if !c.bearerAllowedIsSet {
		c.bearerAllowed = !limits.DisallowBearer
	}

	err = au.UpdateTags(acct.Tags(), c.tags, c.rmTags)
	if err != nil {
		return err
	}

	if jsEnabled {
		if !c.jetStreamIsSet {
			c.jetStream = true
		}

		jsl := limits.JetStreamLimits
		if c.storeMaxString == "" {
			c.storeMaxString = strconv.Itoa(int(jsl.DiskStorage))
		}
		if c.memMaxString == "" {
			c.memMaxString = strconv.Itoa(int(jsl.MemoryStorage))
		}
		if c.memMaxStreamString == "" {
			c.memMaxStreamString = strconv.Itoa(int(jsl.MemoryMaxStreamBytes))
		}
		if c.storeMaxStreamString == "" {
			c.storeMaxStreamString = strconv.Itoa(int(jsl.DiskMaxStreamBytes))
		}
		if !c.maxConsumersIsSet {
			c.maxConsumers = jsl.Consumer
		}
		if !c.maxStreamsIsSet {
			c.maxStreams = jsl.Streams
		}
		if !c.streamSizeRequiredIsSet {
			c.streamSizeRequired = jsl.MaxBytesRequired
		}
		if !c.maxAckPendingIsSet {
			c.maxAckPending = jsl.MaxAckPending
		}
	}

	err = c.parseStringOptions()
	if err != nil {
		return err
	}

	err = c.updateAccount(acct, c.jetStreamIsSet || jsEnabled)
	if err != nil {
		return err
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	return c.fShowAccount(os.Stdout, operator, acct)
}

//func (c *authAccountCommand) rmAction(_ *cobra.Command, _ []string) error {
//	fmt.Println("WARNING: At present deleting is not supported by the nsc store")
//	fmt.Println()
//
//	auth, operator, account, err := c.selectAccount(true)
//	if err != nil {
//		return err
//	}
//
//	if !c.force {
//		ok, err := askConfirmation(fmt.Sprintf("Really remove the Accouint %s", c.accountName), false)
//		if err != nil {
//			return err
//		}
//
//		if !ok {
//			return nil
//		}
//	}
//
//	err = operator.Accounts().Delete(account.Name())
//	if err != nil {
//		return err
//	}
//
//	err = auth.Commit()
//	if err != nil {
//		return err
//	}
//
//	fmt.Printf("Removed account %s\n", account.Name())
//	return nil
//}

func (c *authAccountCommand) lsAction(_ *cobra.Command, args []string) error {
	c.operatorName = argValue(args, 0)

	_, operator, err := c.selectOperator(true)
	if err != nil {
		return err
	}

	list := operator.Accounts().List()
	if len(list) == 0 {
		fmt.Println("No Accounts found")
		return nil
	}

	var names []string
	for _, op := range list {
		names = append(names, op.Name())
	}

	if c.listNames {
		if c.json {
			return iu.PrintJSON(names)
		}

		for _, op := range names {
			fmt.Println(op)
		}
		return nil
	}

	if c.json {
		return iu.PrintJSON(list)
	}

	table := iu.NewTableWriterf(opts(), "Accounts")
	table.AddHeaders("Name", "Subject", "Users", "JetStream", "System")
	for _, acct := range list {
		system := ""
		js := ""
		sa, err := operator.SystemAccount()
		if err == nil && sa != nil && acct.Subject() == sa.Subject() {
			system = "true"
		}
		if acct.Limits().JetStream().IsJetStreamEnabled() {
			js = "true"
		}

		table.AddRow(acct.Name(), acct.Subject(), len(acct.Users().List()), js, system)
	}
	fmt.Println(table.Render())

	return nil
}

func (c *authAccountCommand) infoAction(_ *cobra.Command, args []string) error {
	c.accountName = argValue(args, 0)

	_, operator, account, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	return c.fShowAccount(os.Stdout, operator, account)
}

func (c *authAccountCommand) updateAccount(acct ab.Account, js bool) error {
	limits := acct.Limits().(au.OperatorLimitsManager).OperatorLimits()
	limits.Conn = c.maxConns
	limits.Subs = c.maxSubs
	limits.Payload = c.maxPayload
	limits.LeafNodeConn = c.maxLeafnodes
	limits.Exports = c.maxExports
	limits.Imports = c.maxImports
	limits.DisallowBearer = !c.bearerAllowed
	if js {
		if c.storeMaxStream > 0 {
			limits.JetStreamLimits.DiskMaxStreamBytes = c.storeMaxStream
		}
		if c.memMaxStream > 0 {
			limits.JetStreamLimits.MemoryMaxStreamBytes = c.memMaxStream
		}
		limits.JetStreamLimits.DiskStorage = c.storeMax
		limits.JetStreamLimits.MemoryStorage = c.memMax
		limits.JetStreamLimits.MaxBytesRequired = c.streamSizeRequired
		limits.JetStreamLimits.Consumer = c.maxConsumers
		limits.JetStreamLimits.Streams = c.maxStreams
		limits.JetStreamLimits.MaxAckPending = c.maxAckPending
	}

	err := acct.Limits().(au.OperatorLimitsManager).SetOperatorLimits(limits)
	if err != nil {
		return err
	}

	if c.expiry > 0 {
		err = acct.SetExpiry(time.Now().Add(c.expiry).Unix())
		if err != nil {
			return err
		}
	}

	if c.clusterTrafficIsSet {
		err = acct.SetClusterTraffic(c.clusterTraffic)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *authAccountCommand) parseStringOptions() error {
	var err error

	if c.maxPayloadString != "" {
		c.maxPayload, err = iu.ParseStringAsBytes(c.maxPayloadString, 64)
		if err != nil {
			return err
		}
	}

	if c.jetStream {
		if c.storeMaxString == "" {
			c.storeMax, err = askOneBytes("Maximum JetStream Disk Storage", "1GB", "Maximum amount of disk this account may use, set using --js-disk", "JetStream requires maximum Disk usage set")
			if err != nil {
				return err
			}
		}
		if c.memMaxString == "" {
			c.memMax, err = askOneBytes("Maximum JetStream Memory Storage", "1GB", "Maximum amount of memory this account may use, set using --js-memory", "JetStream requires maximum Memory usage set")
			if err != nil {
				return err
			}
		}

		if c.storeMaxString != "" {
			c.storeMax, err = iu.ParseStringAsBytes(c.storeMaxString, 64)
			if err != nil {
				return err
			}
		}
		if c.memMaxString != "" {
			c.memMax, err = iu.ParseStringAsBytes(c.memMaxString, 64)
			if err != nil {
				return err
			}
		}

		if c.memMaxStreamString != "-1" {
			c.memMaxStream, err = iu.ParseStringAsBytes(c.memMaxStreamString, 64)
			if err != nil {
				return err
			}
		}
		if c.storeMaxStreamString != "-1" {
			c.storeMaxStream, err = iu.ParseStringAsBytes(c.storeMaxStreamString, 64)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *authAccountCommand) addAction(cmd *cobra.Command, args []string) error {
	c.accountName = argValue(args, 0)
	c.bearerAllowedIsSet = cmd.Flags().Changed("bearer")
	c.maxConnsIsSet = cmd.Flags().Changed("connections")
	c.maxExportsIsSet = cmd.Flags().Changed("exports")
	c.maxImportsIsSet = cmd.Flags().Changed("imports")
	c.jetStreamIsSet = cmd.Flags().Changed("jetstream")
	c.maxConsumersIsSet = cmd.Flags().Changed("js-consumers")
	c.maxAckPendingIsSet = cmd.Flags().Changed("js-max-pending")
	c.streamSizeRequiredIsSet = cmd.Flags().Changed("js-stream-size-required")
	c.maxStreamsIsSet = cmd.Flags().Changed("js-streams")
	c.clusterTrafficIsSet = cmd.Flags().Changed("js-cluster-traffic")
	c.maxLeafNodesIsSet = cmd.Flags().Changed("leafnodes")
	c.maxSubIsSet = cmd.Flags().Changed("subscriptions")

	auth, operator, err := c.selectOperator(true)
	if err != nil {
		return err
	}

	if c.accountName == "" {
		err := iu.AskOne(&survey.Input{
			Message: "Account Name",
			Help:    "A unique name for the Account being added",
		}, &c.accountName, survey.WithValidator(survey.Required))
		if err != nil {
			return err
		}
	}

	if au.IsAuthItemKnown(operator.Accounts().List(), c.accountName) {
		return fmt.Errorf("account %s already exist", c.accountName)
	}

	acct, err := operator.Accounts().Add(c.accountName)
	if err != nil {
		return err
	}

	if c.signingKey != "" {
		sk, err := au.SelectSigningKey(acct, c.signingKey)
		if err != nil {
			return err
		}
		c.signingKey = sk.Key()
	}

	if c.signingKey != "" {
		err = acct.SetIssuer(c.signingKey)
		if err != nil {
			return err
		}
	}

	err = au.UpdateTags(acct.Tags(), c.tags, c.rmTags)
	if err != nil {
		return err
	}

	if !c.defaults {
		if c.maxConns == -1 {
			c.maxConns, err = askOneInt("Maximum Connections", "-1", "The maximum amount of client connections allowed for this account, set using --connections")
			if err != nil {
				return err
			}
		}

		if c.maxSubs == -1 {
			c.maxSubs, err = askOneInt("Maximum Subscriptions", "-1", "The maximum amount of subscriptions allowed for this account, set using --subscriptions")
			if err != nil {
				return err
			}
		}

		if c.maxPayloadString == "" {
			c.maxPayload, err = askOneBytes("Maximum Message Payload", "-1", "The maximum size any message may have, set using --payload", "")
			if err != nil {
				return err
			}
			c.maxPayloadString = ""
		}

		fmt.Println()
	}

	err = c.parseStringOptions()
	if err != nil {
		return err
	}

	err = c.updateAccount(acct, c.jetStream)
	if err != nil {
		return err
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	return c.fShowAccount(os.Stdout, operator, acct)
}

func (c *authAccountCommand) fShowAccount(w io.Writer, operator ab.Operator, acct ab.Account) error {
	out, err := c.showAccount(operator, acct)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(w, out)

	return err
}

func (c *authAccountCommand) showAccount(operator ab.Operator, acct ab.Account) (string, error) {
	if c.json {
		return iu.ToJSON(acct)
	}

	limits := acct.Limits()
	js := limits.JetStream()
	serviceExports := len(acct.Exports().Services().List())
	streamExports := len(acct.Exports().Streams().List())
	serviceImports := len(acct.Imports().Services().List())
	streamImports := len(acct.Imports().Streams().List())

	cols := newColumnsf("Account %s (%s)", acct.Name(), acct.Subject())

	cols.AddSectionTitle("Configuration")
	cols.AddRow("Name", acct.Name())
	cols.AddRow("Issuer", acct.Issuer())
	if operator != nil {
		cols.AddRow("Operator", operator.Name())
		sa, err := operator.SystemAccount()
		if err == nil {
			cols.AddRow("System Account", sa.Subject() == acct.Subject())
		} else {
			cols.AddRow("System Account", false)
		}
	}
	if tags, _ := acct.Tags().All(); len(tags) > 0 {
		cols.AddStringsAsValue("Tags", tags)
	}
	cols.AddRow("JetStream", js.IsJetStreamEnabled())
	cols.AddRowIf("Expiry", time.Unix(acct.Expiry(), 0), acct.Expiry() > 0)
	cols.AddRow("Users", len(acct.Users().List()))
	cols.AddRow("Revocations", len(acct.Revocations().List()))
	cols.AddRow("Service Exports", serviceExports)
	cols.AddRow("Stream Exports", streamExports)
	cols.AddRow("Service Imports", serviceImports)
	cols.AddRow("Stream Imports", streamImports)

	cols.AddSectionTitle("Limits")
	cols.AddRow("Bearer Tokens Allowed", !limits.DisallowBearerTokens())
	cols.AddRowUnlimited("Subscriptions", limits.MaxSubscriptions(), -1)
	cols.AddRowUnlimited("Connections", limits.MaxConnections(), -1)
	cols.AddRowUnlimitedIf("Maximum Payload", humanize.IBytes(uint64(limits.MaxPayload())), limits.MaxPayload() <= 0)
	if limits.MaxData() > 0 {
		cols.AddRow("Data", limits.MaxData()) // only showing when set as afaik its a ngs thing
	}
	cols.AddRowUnlimited("Leafnodes", limits.MaxLeafNodeConnections(), -1)
	cols.AddRowUnlimited("Imports", limits.MaxImports(), -1)
	cols.AddRowUnlimited("Exports", limits.MaxExports(), -1)

	if js.IsJetStreamEnabled() {
		cols.Indent(2)
		cols.AddSectionTitle("JetStream Settings")

		traffic := acct.ClusterTraffic()
		if traffic == "" {
			traffic = "system"
		}
		cols.AddRow("Cluster Traffic", traffic)

		tiers := c.validTiers(acct)

		cols.Indent(4)
		for _, tc := range tiers {
			tier, _ := js.Get(tc)
			if tier == nil {
				continue
			}

			if tc == 0 {
				cols.AddSectionTitle("Account Default Limits")
			} else {
				cols.AddSectionTitle("Tier %d", tc)
			}

			if unlimited, _ := tier.IsUnlimited(); unlimited {
				cols.Indent(6)
				cols.Println("Unlimited")
				cols.Indent(4)
				continue
			}

			maxAck, _ := tier.MaxAckPending()
			maxMem, _ := tier.MaxMemoryStorage()
			maxMemStream, _ := tier.MaxMemoryStreamSize()
			maxConns, _ := tier.MaxConsumers()
			maxDisk, _ := tier.MaxDiskStorage()
			maxDiskStream, _ := tier.MaxDiskStreamSize()
			streams, _ := tier.MaxStreams()
			streamSizeRequired, _ := tier.MaxStreamSizeRequired()

			cols.AddRowUnlimited("Max Ack Pending", maxAck, 0)
			cols.AddRowUnlimited("Maximum Streams", streams, -1)
			cols.AddRowUnlimited("Max Consumers Per Stream", maxConns, -1)
			cols.AddRow("Max Stream Size Required", streamSizeRequired)
			cols.AddRow("Max File Storage", humanize.IBytes(uint64(maxDisk)))
			cols.AddRowIf("Max File Storage Stream Size", humanize.IBytes(uint64(maxDiskStream)), maxDiskStream > 0)
			cols.AddRow("Max Memory Storage", humanize.IBytes(uint64(maxMem)))
			cols.AddRowIf("Max Memory Storage Stream Size", humanize.IBytes(uint64(maxMemStream)), maxMemStream > 0)
		}

		cols.Indent(0)
	}

	return cols.Render()
}

func (c *authAccountCommand) validTiers(acct ab.Account) []int8 {
	tiers := []int8{}
	for i := int8(0); i <= 5; i++ {
		tier, _ := acct.Limits().JetStream().Get(i)
		if tier != nil {
			tiers = append(tiers, i)
		}
	}

	if len(tiers) > 1 {
		tiers = tiers[1:]
	}

	return tiers
}

func (c *authAccountCommand) loadMappingsConfig() (map[string][]ab.Mapping, error) {
	if c.inputFile != "" {
		f, err := os.ReadFile(c.inputFile)
		if err != nil {
			return nil, err
		}

		var mappings map[string][]ab.Mapping
		err = yaml.Unmarshal(f, &mappings)
		if err != nil {
			return nil, fmt.Errorf("unable to load config file: %s", err)
		}
		return mappings, nil
	}
	return nil, nil

}

func (c *authAccountCommand) mappingAddAction(_ *cobra.Command, args []string) error {
	c.accountName = argValue(args, 0)
	c.mapSource = argValue(args, 1)
	c.mapTarget = argValue(args, 2)
	c.mapWeight = 100
	if v := argValue(args, 3); v != "" {
		w, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return err
		}
		c.mapWeight = uint(w)
	}
	c.mapCluster = argValue(args, 4)

	var err error
	mappings := map[string][]ab.Mapping{}
	if c.inputFile != "" {
		mappings, err = c.loadMappingsConfig()
		if err != nil {
			return err
		}
	}

	auth, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	if c.inputFile == "" {
		if c.mapSource == "" {
			err := iu.AskOne(&survey.Input{
				Message: "Source subject",
				Help:    "The source subject of the mapping",
			}, &c.mapSource, survey.WithValidator(survey.Required))
			if err != nil {
				return err
			}
		}

		if c.mapTarget == "" {
			err := iu.AskOne(&survey.Input{
				Message: "Target subject",
				Help:    "The target subject of the mapping",
			}, &c.mapTarget, survey.WithValidator(survey.Required))
			if err != nil {
				return err
			}
		}

		mapping := ab.Mapping{Subject: c.mapTarget, Weight: uint8(c.mapWeight)}
		if c.mapCluster != "" {
			mapping.Cluster = c.mapCluster
		}
		// check if there are mappings already set for the source
		currentMappings := acct.SubjectMappings().Get(c.mapSource)
		if len(currentMappings) > 0 {
			// Check that we don't overwrite the current mapping
			for _, m := range currentMappings {
				if m.Subject == c.mapTarget {
					return fmt.Errorf("mapping %s -> %s already exists", c.mapSource, c.mapTarget)
				}
			}
		}
		currentMappings = append(currentMappings, mapping)
		mappings[c.mapSource] = currentMappings
	}

	for subject, m := range mappings {
		err = acct.SubjectMappings().Set(subject, m...)
		if err != nil {
			return err
		}
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	return c.fShowMappings(os.Stdout, mappings)
}

func (c *authAccountCommand) mappingInfoAction(_ *cobra.Command, args []string) error {
	c.accountName = argValue(args, 0)
	c.mapSource = argValue(args, 1)

	_, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	accountMappings := acct.SubjectMappings().List()
	if len(accountMappings) == 0 {
		fmt.Println("No mappings defined")
		return nil
	}

	if c.mapSource == "" {
		err = iu.AskOne(&survey.Select{
			Message:  "Select a mapping to inspect",
			Options:  accountMappings,
			PageSize: iu.SelectPageSize(len(accountMappings)),
		}, &c.mapSource)
		if err != nil {
			return err
		}
	}

	mappings := map[string][]ab.Mapping{
		c.mapSource: acct.SubjectMappings().Get(c.mapSource),
	}

	return c.fShowMappings(os.Stdout, mappings)
}

func (c *authAccountCommand) mappingListAction(_ *cobra.Command, args []string) error {
	c.accountName = argValue(args, 0)

	_, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	mappings := acct.SubjectMappings().List()
	if len(mappings) == 0 {
		fmt.Println("No mappings defined")
		return nil
	}

	tbl := iu.NewTableWriterf(opts(), "Subject mappings for account %s", acct.Name())
	tbl.AddHeaders("Source Subject", "Target Subject", "Weight", "Cluster")

	for _, fromMapping := range acct.SubjectMappings().List() {
		subjectMaps := acct.SubjectMappings().Get(fromMapping)
		for _, m := range subjectMaps {
			tbl.AddRow(fromMapping, m.Subject, m.Weight, m.Cluster)
		}
	}

	fmt.Println(tbl.Render())
	return nil
}

func (c *authAccountCommand) mappingRmAction(_ *cobra.Command, args []string) error {
	c.accountName = argValue(args, 0)
	c.mapSource = argValue(args, 1)

	auth, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	mappings := acct.SubjectMappings().List()
	if len(mappings) == 0 {
		fmt.Println("No mappings defined")
		return nil
	}

	if c.mapSource == "" {
		err = iu.AskOne(&survey.Select{
			Message:  "Select a mapping to delete",
			Options:  mappings,
			PageSize: iu.SelectPageSize(len(mappings)),
		}, &c.mapSource)
		if err != nil {
			return err
		}
	}

	err = acct.SubjectMappings().Delete(c.mapSource)
	if err != nil {
		return err
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	fmt.Printf("Deleted mapping {%s}\n", c.mapSource)
	return nil
}

func (c *authAccountCommand) fShowMappings(w io.Writer, mappings map[string][]ab.Mapping) error {
	out, err := c.showMappings(mappings)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(w, out)
	return err
}

func (c *authAccountCommand) showMappings(mappings map[string][]ab.Mapping) (string, error) {
	if c.json {
		return iu.ToJSON(mappings)
	}

	cols := newColumnsf("Subject mappings")
	cols.AddSectionTitle("Configuration")
	for source, m := range mappings {
		totalWeight := 0
		for _, wm := range m {
			cols.AddRow("Source", source)
			cols.AddRow("Target", wm.Subject)
			cols.AddRow("Weight", wm.Weight)
			cols.AddRow("Cluster", wm.Cluster)
			cols.AddRow("", "")
			totalWeight += int(wm.Weight)
		}
		cols.AddRow("Total weight:", totalWeight)
		cols.AddRow("", "")
	}

	return cols.Render()
}
